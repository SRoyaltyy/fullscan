# Factor mine action — `union_coil_off_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ coil_off, no 🚨

Cash book **-7.72%** ($9,228) · signal-only (no cash/fees) was -10.21%. Starts YES **0/20**. Fills 94 · skips 188 · realized $-771.65.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior 5-session return is at least 0%.
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-have: prior relative volume is at least 0.7.
- Must-have: prior relative volume is at most 2.2 (not a blow-off).
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
- **Gate** `ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,228.35.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TPG` | 98 | — | $50.62 | +0.00 | $54.62 | +391.69 | +391.69 | +0.00 | +391.69 |
| 2026-08-13 | `VOR` | 227 | — | $22.01 | +0.00 | $23.29 | +290.56 | +290.56 | +0.00 | +290.56 |
| 2026-08-14 | `TPG` | 98 | $54.62 | $55.29 | +65.66 | $53.03 | -221.48 | -155.82 | +457.35 | +235.87 |
| 2026-08-14 | `VOR` | 227 | $23.29 | $23.33 | +9.08 | $23.03 | -68.10 | -59.02 | +299.64 | +231.54 |
| 2026-08-14 | `LDI` | 4 | — | $0.94 | +0.00 | $0.90 | -0.16 | -0.16 | +0.00 | -0.16 |
| 2026-08-14 | `BTBT` | 3 | — | $1.50 | +0.00 | $1.57 | +0.21 | +0.21 | +0.00 | +0.21 |
| 2026-08-14 | `ANGX` | 1 | — | $4.31 | +0.00 | $4.37 | +0.06 | +0.06 | +0.00 | +0.06 |
| 2026-08-14 | `HYLN` | 1 | — | $4.18 | +0.00 | $4.06 | -0.12 | -0.12 | +0.00 | -0.12 |
| 2026-08-17 | `TPG` | 98 | $53.03 | $52.67 | -35.28 | $51.77 | -88.20 | -123.48 | +200.59 | +112.39 |
| 2026-08-17 | `VOR` | 227 | $23.03 | $22.91 | -27.24 | $23.01 | +22.70 | -4.54 | +204.30 | +227.00 |
| 2026-08-17 | `LDI` | 4 | $0.90 | $0.91 | +0.04 | $0.88 | -0.13 | -0.09 | -0.12 | -0.25 |
| 2026-08-17 | `BTBT` | 3 | $1.57 | $1.52 | -0.15 | $1.60 | +0.24 | +0.09 | +0.06 | +0.30 |
| 2026-08-17 | `ANGX` | 1 | $4.37 | $4.60 | +0.23 | $4.71 | +0.11 | +0.34 | +0.29 | +0.40 |
| 2026-08-17 | `HYLN` | 1 | $4.06 | $4.10 | +0.04 | $4.09 | -0.01 | +0.03 | -0.08 | -0.09 |
| 2026-08-17 | `DNN` | 1 | — | $3.24 | +0.00 | $3.19 | -0.05 | -0.05 | +0.00 | -0.05 |
| 2026-08-18 | `TPG` | 98 | $51.77 | $51.77 | +0.00 | — | +0.00 | +0.00 | +112.39 | — |
| 2026-08-18 | `VOR` | 227 | $23.01 | $22.82 | -43.13 | — | +0.00 | -43.13 | +183.87 | — |
| 2026-08-18 | `LDI` | 4 | $0.88 | $0.87 | -0.02 | $0.86 | -0.05 | -0.07 | -0.27 | -0.32 |
| 2026-08-18 | `BTBT` | 3 | $1.60 | $1.54 | -0.18 | $1.45 | -0.27 | -0.45 | +0.12 | -0.15 |
| 2026-08-18 | `ANGX` | 1 | $4.71 | $4.79 | +0.08 | $4.85 | +0.06 | +0.14 | +0.48 | +0.54 |
| 2026-08-18 | `HYLN` | 1 | $4.09 | $3.95 | -0.14 | $3.86 | -0.09 | -0.23 | -0.23 | -0.32 |
| 2026-08-18 | `DNN` | 1 | $3.19 | $3.11 | -0.08 | $3.15 | +0.04 | -0.04 | -0.13 | -0.09 |
| 2026-08-19 | `LDI` | 4 | $0.86 | $0.88 | +0.09 | — | +0.00 | +0.09 | -0.23 | — |
| 2026-08-19 | `BTBT` | 3 | $1.45 | $1.42 | -0.09 | — | +0.00 | -0.09 | -0.24 | — |
| 2026-08-19 | `ANGX` | 1 | $4.85 | $4.79 | -0.06 | — | +0.00 | -0.06 | +0.48 | — |
| 2026-08-19 | `HYLN` | 1 | $3.86 | $3.87 | +0.01 | — | +0.00 | +0.01 | -0.31 | — |
| 2026-08-19 | `DNN` | 1 | $3.15 | $3.19 | +0.04 | $3.22 | +0.03 | +0.07 | -0.05 | -0.02 |
| 2026-08-20 | `DNN` | 1 | $3.22 | $3.20 | -0.02 | — | +0.00 | -0.02 | -0.04 | — |
| 2026-08-20 | `AG` | 62 | — | $20.55 | +0.00 | $21.19 | +39.68 | +39.68 | +0.00 | +39.68 |
| 2026-08-20 | `BHP` | 14 | — | $91.01 | +0.00 | $93.63 | +36.68 | +36.68 | +0.00 | +36.68 |
| 2026-08-20 | `HDSN` | 222 | — | $5.77 | +0.00 | $5.57 | -44.40 | -44.40 | +0.00 | -44.40 |
| 2026-08-20 | `IAG` | 65 | — | $19.63 | +0.00 | $20.50 | +56.55 | +56.55 | +0.00 | +56.55 |
| 2026-08-20 | `KGC` | 43 | — | $29.63 | +0.00 | $31.43 | +77.40 | +77.40 | +0.00 | +77.40 |
| 2026-08-20 | `NFGC` | 734 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `DNA` | 172 | — | $7.45 | +0.00 | $6.96 | -84.28 | -84.28 | +0.00 | -84.28 |
| 2026-08-20 | `EXK` | 119 | — | $10.77 | +0.00 | $10.97 | +23.80 | +23.80 | +0.00 | +23.80 |
| 2026-08-21 | `AG` | 62 | $21.19 | $21.90 | +44.02 | $21.09 | -50.22 | -6.20 | +83.70 | +33.48 |
| 2026-08-21 | `BHP` | 14 | $93.63 | $95.72 | +29.26 | $97.03 | +18.34 | +47.60 | +65.94 | +84.28 |
| 2026-08-21 | `HDSN` | 222 | $5.57 | $5.67 | +22.20 | $5.63 | -8.88 | +13.32 | -22.20 | -31.08 |
| 2026-08-21 | `IAG` | 65 | $20.50 | $21.17 | +43.55 | $21.14 | -1.95 | +41.60 | +100.10 | +98.15 |
| 2026-08-21 | `KGC` | 43 | $31.43 | $32.17 | +31.82 | $32.76 | +25.37 | +57.19 | +109.22 | +134.59 |
| 2026-08-21 | `NFGC` | 734 | $1.75 | $1.79 | +29.36 | $1.84 | +36.70 | +66.06 | +29.36 | +66.06 |
| 2026-08-21 | `DNA` | 172 | $6.96 | $7.09 | +22.36 | $7.40 | +53.32 | +75.68 | -61.92 | -8.60 |
| 2026-08-21 | `EXK` | 119 | $10.97 | $11.34 | +44.03 | $10.62 | -85.68 | -41.65 | +67.83 | -17.85 |
| 2026-08-21 | `BTBT` | 2 | — | $1.66 | +0.00 | $1.53 | -0.26 | -0.26 | +0.00 | -0.26 |
| 2026-08-21 | `ORBS` | 4 | — | $0.86 | +0.00 | $0.88 | +0.06 | +0.06 | +0.00 | +0.06 |
| 2026-08-24 | `AG` | 62 | $21.09 | $21.30 | +13.02 | $20.83 | -29.14 | -16.12 | +46.50 | +17.36 |
| 2026-08-24 | `BHP` | 14 | $97.03 | $97.31 | +3.92 | $97.13 | -2.52 | +1.40 | +88.20 | +85.68 |
| 2026-08-24 | `HDSN` | 222 | $5.63 | $5.69 | +13.32 | $5.52 | -37.74 | -24.42 | -17.76 | -55.50 |
| 2026-08-24 | `IAG` | 65 | $21.14 | $21.38 | +15.60 | $21.80 | +27.30 | +42.90 | +113.75 | +141.05 |
| 2026-08-24 | `KGC` | 43 | $32.76 | $33.03 | +11.61 | $32.98 | -2.15 | +9.46 | +146.20 | +144.05 |
| 2026-08-24 | `NFGC` | 734 | $1.84 | $1.86 | +14.68 | $1.90 | +29.36 | +44.04 | +80.74 | +110.10 |
| 2026-08-24 | `DNA` | 172 | $7.40 | $7.25 | -25.80 | $6.78 | -80.84 | -106.64 | -34.40 | -115.24 |
| 2026-08-24 | `EXK` | 119 | $10.62 | $10.97 | +41.65 | $10.76 | -24.99 | +16.66 | +23.80 | -1.19 |
| 2026-08-24 | `BTBT` | 2 | $1.53 | $1.55 | +0.04 | $1.51 | -0.08 | -0.04 | -0.22 | -0.30 |
| 2026-08-24 | `ORBS` | 4 | $0.88 | $0.89 | +0.04 | $0.84 | -0.20 | -0.16 | +0.10 | -0.10 |
| 2026-08-25 | `AG` | 62 | $20.83 | $20.32 | -31.62 | — | +0.00 | -31.62 | -14.26 | — |
| 2026-08-25 | `BHP` | 14 | $97.13 | $95.86 | -17.78 | — | +0.00 | -17.78 | +67.90 | — |
| 2026-08-25 | `HDSN` | 222 | $5.52 | $5.53 | +2.22 | — | +0.00 | +2.22 | -53.28 | — |
| 2026-08-25 | `IAG` | 65 | $21.80 | $21.21 | -38.35 | — | +0.00 | -38.35 | +102.70 | — |
| 2026-08-25 | `KGC` | 43 | $32.98 | $32.32 | -28.38 | — | +0.00 | -28.38 | +115.67 | — |
| 2026-08-25 | `NFGC` | 734 | $1.90 | $1.90 | +0.00 | — | +0.00 | +0.00 | +110.10 | — |
| 2026-08-25 | `DNA` | 172 | $6.78 | $6.94 | +27.52 | — | +0.00 | +27.52 | -87.72 | — |
| 2026-08-25 | `EXK` | 119 | $10.76 | $10.44 | -38.08 | — | +0.00 | -38.08 | -39.27 | — |
| 2026-08-25 | `BTBT` | 2 | $1.51 | $1.51 | +0.00 | $1.58 | +0.14 | +0.14 | -0.30 | -0.16 |
| 2026-08-25 | `ORBS` | 4 | $0.84 | $0.83 | -0.04 | $0.80 | -0.12 | -0.16 | -0.14 | -0.26 |
| 2026-08-25 | `OCUL` | 118 | — | $10.98 | +0.00 | $10.88 | -11.80 | -11.80 | +0.00 | -11.80 |
| 2026-08-25 | `RZLT` | 263 | — | $4.94 | +0.00 | $5.01 | +18.41 | +18.41 | +0.00 | +18.41 |
| 2026-08-25 | `HCA` | 3 | — | $426.97 | +0.00 | $428.76 | +5.37 | +5.37 | +0.00 | +5.37 |
| 2026-08-25 | `KURA` | 95 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `LIFE` | 35 | — | $36.96 | +0.00 | $38.56 | +56.00 | +56.00 | +0.00 | +56.00 |
| 2026-08-25 | `AMTX` | 686 | — | $1.90 | +0.00 | $1.91 | +6.86 | +6.86 | +0.00 | +6.86 |
| 2026-08-25 | `AVAH` | 95 | — | $13.62 | +0.00 | $13.59 | -3.33 | -3.33 | +0.00 | -3.33 |
| 2026-08-25 | `ETON` | 20 | — | $64.55 | +0.00 | $63.05 | -30.00 | -30.00 | +0.00 | -30.00 |
| 2026-08-26 | `BTBT` | 2 | $1.58 | $1.53 | -0.10 | — | +0.00 | -0.10 | -0.26 | — |
| 2026-08-26 | `ORBS` | 4 | $0.80 | $0.80 | -0.02 | — | +0.00 | -0.02 | -0.27 | — |
| 2026-08-26 | `OCUL` | 118 | $10.88 | $10.79 | -10.62 | $10.77 | -2.36 | -12.98 | -22.42 | -24.78 |
| 2026-08-26 | `RZLT` | 263 | $5.01 | $5.01 | +0.00 | $5.04 | +7.89 | +7.89 | +18.41 | +26.30 |
| 2026-08-26 | `HCA` | 3 | $428.76 | $427.50 | -3.78 | $427.16 | -1.02 | -4.80 | +1.59 | +0.57 |
| 2026-08-26 | `KURA` | 95 | $13.59 | $13.63 | +3.80 | $13.06 | -54.15 | -50.35 | +3.80 | -50.35 |
| 2026-08-26 | `LIFE` | 35 | $38.56 | $38.24 | -11.20 | $39.11 | +30.45 | +19.25 | +44.80 | +75.25 |
| 2026-08-26 | `AMTX` | 686 | $1.91 | $1.91 | +0.00 | $1.88 | -20.58 | -20.58 | +6.86 | -13.72 |
| 2026-08-26 | `AVAH` | 95 | $13.59 | $13.65 | +5.70 | $13.62 | -2.85 | +2.85 | +2.38 | -0.48 |
| 2026-08-26 | `ETON` | 20 | $63.05 | $63.60 | +11.00 | $62.62 | -19.60 | -8.60 | -19.00 | -38.60 |
| 2026-08-26 | `ACRS` | 1 | — | $6.53 | +0.00 | $6.19 | -0.34 | -0.34 | +0.00 | -0.34 |
| 2026-08-26 | `TMCI` | 1 | — | $4.78 | +0.00 | $4.72 | -0.06 | -0.06 | +0.00 | -0.06 |
| 2026-08-26 | `CRDL` | 4 | — | $2.03 | +0.00 | $2.14 | +0.44 | +0.44 | +0.00 | +0.44 |
| 2026-08-27 | `OCUL` | 118 | $10.77 | $10.63 | -16.52 | $10.82 | +22.42 | +5.90 | -41.30 | -18.88 |
| 2026-08-27 | `RZLT` | 263 | $5.04 | $5.07 | +7.89 | $4.98 | -23.67 | -15.78 | +34.19 | +10.52 |
| 2026-08-27 | `HCA` | 3 | $427.16 | $424.61 | -7.65 | $419.34 | -15.81 | -23.46 | -7.08 | -22.89 |
| 2026-08-27 | `KURA` | 95 | $13.06 | $12.98 | -7.60 | $13.18 | +19.00 | +11.40 | -57.95 | -38.95 |
| 2026-08-27 | `LIFE` | 35 | $39.11 | $39.40 | +10.15 | $39.44 | +1.40 | +11.55 | +85.40 | +86.80 |
| 2026-08-27 | `AMTX` | 686 | $1.88 | $1.87 | -6.86 | $1.87 | +0.00 | -6.86 | -20.58 | -20.58 |
| 2026-08-27 | `AVAH` | 95 | $13.62 | $13.62 | +0.00 | $13.82 | +19.00 | +19.00 | -0.48 | +18.53 |
| 2026-08-27 | `ETON` | 20 | $62.62 | $62.50 | -2.40 | $62.67 | +3.40 | +1.00 | -41.00 | -37.60 |
| 2026-08-27 | `ACRS` | 1 | $6.19 | $6.15 | -0.04 | $6.16 | +0.01 | -0.03 | -0.38 | -0.37 |
| 2026-08-27 | `TMCI` | 1 | $4.72 | $4.72 | +0.00 | $4.60 | -0.12 | -0.12 | -0.06 | -0.18 |
| 2026-08-27 | `CRDL` | 4 | $2.14 | $2.09 | -0.20 | $2.06 | -0.12 | -0.32 | +0.24 | +0.12 |
| 2026-08-28 | `OCUL` | 118 | $10.82 | $10.97 | +17.70 | — | +0.00 | +17.70 | -1.18 | — |
| 2026-08-28 | `RZLT` | 263 | $4.98 | $4.95 | -7.89 | — | +0.00 | -7.89 | +2.63 | — |
| 2026-08-28 | `HCA` | 3 | $419.34 | $423.76 | +13.26 | — | +0.00 | +13.26 | -9.63 | — |
| 2026-08-28 | `KURA` | 95 | $13.18 | $13.05 | -12.35 | — | +0.00 | -12.35 | -51.30 | — |
| 2026-08-28 | `LIFE` | 35 | $39.44 | $39.60 | +5.60 | — | +0.00 | +5.60 | +92.40 | — |
| 2026-08-28 | `AMTX` | 686 | $1.87 | $1.89 | +13.72 | — | +0.00 | +13.72 | -6.86 | — |
| 2026-08-28 | `AVAH` | 95 | $13.82 | $13.90 | +7.60 | — | +0.00 | +7.60 | +26.13 | — |
| 2026-08-28 | `ETON` | 20 | $62.67 | $61.98 | -13.80 | — | +0.00 | -13.80 | -51.40 | — |
| 2026-08-28 | `ACRS` | 1 | $6.16 | $6.10 | -0.06 | $6.01 | -0.09 | -0.15 | -0.43 | -0.52 |
| 2026-08-28 | `TMCI` | 1 | $4.60 | $4.65 | +0.05 | $4.65 | +0.00 | +0.05 | -0.13 | -0.13 |
| 2026-08-28 | `CRDL` | 4 | $2.06 | $2.06 | +0.00 | $1.94 | -0.48 | -0.48 | +0.12 | -0.36 |
| 2026-08-28 | `CRK` | 101 | — | $14.63 | +0.00 | $14.29 | -34.34 | -34.34 | +0.00 | -34.34 |
| 2026-08-28 | `GRRR` | 94 | — | $15.66 | +0.00 | $14.41 | -117.50 | -117.50 | +0.00 | -117.50 |
| 2026-08-28 | `TTMI` | 12 | — | $122.81 | +0.00 | $118.65 | -49.92 | -49.92 | +0.00 | -49.92 |
| 2026-08-28 | `EQ` | 601 | — | $2.46 | +0.00 | $2.39 | -42.07 | -42.07 | +0.00 | -42.07 |
| 2026-08-28 | `BTSG` | 24 | — | $60.54 | +0.00 | $59.13 | -33.84 | -33.84 | +0.00 | -33.84 |
| 2026-08-28 | `ZYME` | 51 | — | $28.91 | +0.00 | $28.27 | -32.64 | -32.64 | +0.00 | -32.64 |
| 2026-08-28 | `ADBT` | 296 | — | $4.99 | +0.00 | $4.99 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-31 | `ACRS` | 1 | $6.01 | $5.97 | -0.04 | — | +0.00 | -0.04 | -0.56 | — |
| 2026-08-31 | `TMCI` | 1 | $4.65 | $4.60 | -0.05 | — | +0.00 | -0.05 | -0.18 | — |
| 2026-08-31 | `CRDL` | 4 | $1.94 | $1.92 | -0.08 | — | +0.00 | -0.08 | -0.44 | — |
| 2026-08-31 | `CRK` | 101 | $14.29 | $14.54 | +25.25 | $14.43 | -11.11 | +14.14 | -9.09 | -20.20 |
| 2026-08-31 | `GRRR` | 94 | $14.41 | $14.44 | +2.82 | $15.04 | +56.40 | +59.22 | -114.68 | -58.28 |
| 2026-08-31 | `TTMI` | 12 | $118.65 | $118.83 | +2.16 | $118.92 | +1.08 | +3.24 | -47.76 | -46.68 |
| 2026-08-31 | `EQ` | 601 | $2.39 | $2.39 | +0.00 | $2.27 | -72.12 | -72.12 | -42.07 | -114.19 |
| 2026-08-31 | `BTSG` | 24 | $59.13 | $58.76 | -8.88 | $58.40 | -8.64 | -17.52 | -42.72 | -51.36 |
| 2026-08-31 | `ZYME` | 51 | $28.27 | $28.06 | -10.71 | $29.41 | +68.85 | +58.14 | -43.35 | +25.50 |
| 2026-08-31 | `ADBT` | 296 | $4.99 | $4.94 | -14.80 | $4.54 | -118.40 | -133.20 | -14.80 | -133.20 |
| 2026-09-01 | `CRK` | 101 | $14.43 | $15.82 | +140.39 | $16.02 | +20.20 | +160.59 | +120.19 | +140.39 |
| 2026-09-01 | `GRRR` | 94 | $15.04 | $14.75 | -27.26 | $14.09 | -62.04 | -89.30 | -85.54 | -147.58 |
| 2026-09-01 | `TTMI` | 12 | $118.92 | $116.68 | -26.88 | $115.33 | -16.20 | -43.08 | -73.56 | -89.76 |
| 2026-09-01 | `EQ` | 601 | $2.27 | $2.25 | -12.02 | $2.21 | -24.04 | -36.06 | -126.21 | -150.25 |
| 2026-09-01 | `BTSG` | 24 | $58.40 | $58.55 | +3.60 | $59.16 | +14.64 | +18.24 | -47.76 | -33.12 |
| 2026-09-01 | `ZYME` | 51 | $29.41 | $29.32 | -4.59 | $29.67 | +17.85 | +13.26 | +20.91 | +38.76 |
| 2026-09-01 | `ADBT` | 296 | $4.54 | $4.45 | -26.64 | $3.68 | -227.92 | -254.56 | -159.84 | -387.76 |
| 2026-09-02 | `CRK` | 101 | $16.02 | $15.70 | -32.32 | — | +0.00 | -32.32 | +108.07 | — |
| 2026-09-02 | `GRRR` | 94 | $14.09 | $13.92 | -15.98 | — | +0.00 | -15.98 | -163.56 | — |
| 2026-09-02 | `TTMI` | 12 | $115.33 | $114.22 | -13.32 | — | +0.00 | -13.32 | -103.08 | — |
| 2026-09-02 | `EQ` | 601 | $2.21 | $2.20 | -6.01 | — | +0.00 | -6.01 | -156.26 | — |
| 2026-09-02 | `BTSG` | 24 | $59.16 | $59.16 | +0.00 | — | +0.00 | +0.00 | -33.12 | — |
| 2026-09-02 | `ZYME` | 51 | $29.67 | $30.00 | +16.83 | — | +0.00 | +16.83 | +55.59 | — |
| 2026-09-02 | `ADBT` | 296 | $3.68 | $3.61 | -20.72 | — | +0.00 | -20.72 | -408.48 | — |
| 2026-09-03 | `ATRC` | 22 | — | $52.88 | +0.00 | $52.46 | -9.24 | -9.24 | +0.00 | -9.24 |
| 2026-09-03 | `HRMY` | 28 | — | $42.93 | +0.00 | $41.86 | -29.96 | -29.96 | +0.00 | -29.96 |
| 2026-09-03 | `CABA` | 331 | — | $3.63 | +0.00 | $3.48 | -49.65 | -49.65 | +0.00 | -49.65 |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `CRK` | 77 | — | $15.45 | +0.00 | $14.95 | -38.50 | -38.50 | +0.00 | -38.50 |
| 2026-09-03 | `ARCT` | 71 | — | $16.77 | +0.00 | $15.56 | -85.91 | -85.91 | +0.00 | -85.91 |
| 2026-09-03 | `CRDL` | 552 | — | $2.18 | +0.00 | $2.16 | -11.04 | -11.04 | +0.00 | -11.04 |
| 2026-09-03 | `SDGR` | 57 | — | $21.03 | +0.00 | $20.71 | -18.24 | -18.24 | +0.00 | -18.24 |
| 2026-09-04 | `ATRC` | 22 | $52.46 | $52.03 | -9.46 | $51.52 | -11.22 | -20.68 | -18.70 | -29.92 |
| 2026-09-04 | `HRMY` | 28 | $41.86 | $41.50 | -10.08 | $42.25 | +21.00 | +10.92 | -40.04 | -19.04 |
| 2026-09-04 | `CABA` | 331 | $3.48 | $3.46 | -6.62 | $3.47 | +3.31 | -3.31 | -56.27 | -52.96 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | $130.22 | +1.71 | -3.69 | -21.78 | -20.07 |
| 2026-09-04 | `CRK` | 77 | $14.95 | $15.00 | +3.85 | $15.26 | +20.02 | +23.87 | -34.65 | -14.63 |
| 2026-09-04 | `ARCT` | 71 | $15.56 | $15.61 | +3.55 | $15.82 | +14.91 | +18.46 | -82.36 | -67.45 |
| 2026-09-04 | `CRDL` | 552 | $2.16 | $2.16 | +0.00 | $2.20 | +22.08 | +22.08 | -11.04 | +11.04 |
| 2026-09-04 | `SDGR` | 57 | $20.71 | $20.58 | -7.41 | $20.09 | -27.93 | -35.34 | -25.65 | -53.58 |
| 2026-09-04 | `ALEC` | 5 | — | $2.52 | +0.00 | $2.46 | -0.30 | -0.30 | +0.00 | -0.30 |
| 2026-09-04 | `BHC` | 2 | — | $6.71 | +0.00 | $6.56 | -0.30 | -0.30 | +0.00 | -0.30 |
| 2026-09-04 | `OABI` | 2 | — | $4.78 | +0.00 | $4.33 | -0.90 | -0.90 | +0.00 | -0.90 |
| 2026-09-04 | `VIR` | 1 | — | $11.31 | +0.00 | $11.38 | +0.07 | +0.07 | +0.00 | +0.07 |
| 2026-09-08 | `ATRC` | 22 | $51.52 | $54.31 | +61.38 | $53.73 | -12.76 | +48.62 | +31.46 | +18.70 |
| 2026-09-08 | `HRMY` | 28 | $42.25 | $42.20 | -1.40 | $42.07 | -3.64 | -5.04 | -20.44 | -24.08 |
| 2026-09-08 | `CABA` | 331 | $3.47 | $3.43 | -13.24 | $3.27 | -52.96 | -66.20 | -66.20 | -119.16 |
| 2026-09-08 | `RVTY` | 9 | $130.22 | $128.50 | -15.48 | $127.08 | -12.78 | -28.26 | -35.55 | -48.33 |
| 2026-09-08 | `CRK` | 77 | $15.26 | $15.50 | +18.48 | $15.16 | -26.18 | -7.70 | +3.85 | -22.33 |
| 2026-09-08 | `ARCT` | 71 | $15.82 | $15.47 | -24.85 | $15.63 | +11.36 | -13.49 | -92.30 | -80.94 |
| 2026-09-08 | `CRDL` | 552 | $2.20 | $2.20 | +0.00 | $2.22 | +11.04 | +11.04 | +11.04 | +22.08 |
| 2026-09-08 | `SDGR` | 57 | $20.09 | $19.87 | -12.54 | $20.03 | +9.12 | -3.42 | -66.12 | -57.00 |
| 2026-09-08 | `ALEC` | 5 | $2.46 | $2.38 | -0.40 | $2.47 | +0.45 | +0.05 | -0.70 | -0.25 |
| 2026-09-08 | `BHC` | 2 | $6.56 | $6.57 | +0.02 | $6.43 | -0.28 | -0.26 | -0.28 | -0.56 |
| 2026-09-08 | `OABI` | 2 | $4.33 | $4.30 | -0.06 | $4.24 | -0.12 | -0.18 | -0.96 | -1.08 |
| 2026-09-08 | `VIR` | 1 | $11.38 | $11.22 | -0.16 | $11.18 | -0.04 | -0.20 | -0.09 | -0.13 |
| 2026-09-09 | `ATRC` | 22 | $53.73 | $53.16 | -12.54 | — | +0.00 | -12.54 | +6.16 | — |
| 2026-09-09 | `HRMY` | 28 | $42.07 | $42.01 | -1.68 | — | +0.00 | -1.68 | -25.76 | — |
| 2026-09-09 | `CABA` | 331 | $3.27 | $3.28 | +3.31 | — | +0.00 | +3.31 | -115.85 | — |
| 2026-09-09 | `RVTY` | 9 | $127.08 | $125.77 | -11.79 | — | +0.00 | -11.79 | -60.12 | — |
| 2026-09-09 | `CRK` | 77 | $15.16 | $15.16 | +0.00 | — | +0.00 | +0.00 | -22.33 | — |
| 2026-09-09 | `ARCT` | 71 | $15.63 | $15.46 | -12.07 | — | +0.00 | -12.07 | -93.01 | — |
| 2026-09-09 | `CRDL` | 552 | $2.22 | $2.22 | +0.00 | — | +0.00 | +0.00 | +22.08 | — |
| 2026-09-09 | `SDGR` | 57 | $20.03 | $19.88 | -8.55 | — | +0.00 | -8.55 | -65.55 | — |
| 2026-09-09 | `ALEC` | 5 | $2.47 | $2.47 | +0.00 | $2.27 | -1.00 | -1.00 | -0.25 | -1.25 |
| 2026-09-09 | `BHC` | 2 | $6.43 | $6.38 | -0.10 | $6.16 | -0.44 | -0.54 | -0.66 | -1.10 |
| 2026-09-09 | `OABI` | 2 | $4.24 | $4.21 | -0.06 | $4.01 | -0.39 | -0.45 | -1.14 | -1.53 |
| 2026-09-09 | `VIR` | 1 | $11.18 | $11.04 | -0.14 | $10.81 | -0.23 | -0.37 | -0.27 | -0.50 |
| 2026-09-10 | `ALEC` | 5 | $2.27 | $2.27 | +0.00 | — | +0.00 | +0.00 | -1.25 | — |
| 2026-09-10 | `BHC` | 2 | $6.16 | $6.19 | +0.06 | — | +0.00 | +0.06 | -1.04 | — |
| 2026-09-10 | `OABI` | 2 | $4.01 | $4.01 | -0.01 | — | +0.00 | -0.01 | -1.54 | — |
| 2026-09-10 | `VIR` | 1 | $10.81 | $10.88 | +0.07 | — | +0.00 | +0.07 | -0.43 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +682.25 | TPG, VOR | — | $37.44 | $10,677.03 | TPG×98, VOR×227 |
| 2026-08-14 | +5.50 | $37.44 | TPG×98, VOR×227 | $10,751.77 | +74.74 | -289.59 | LDI, BTBT, ANGX, HYLN | — | $20.51 | $10,461.99 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 |
| 2026-08-17 | +2.25 | $20.51 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1 | $10,399.63 | -62.36 | -65.34 | DNN | — | $17.24 | $10,334.26 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 |
| 2026-08-18 | -6.20 | $17.24 | TPG×98, VOR×227, LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | $10,290.79 | -43.47 | -0.31 | — | TPG, VOR | $10,265.49 | $10,285.13 | LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 |
| 2026-08-19 | -7.20 | $10,265.49 | LDI×4, BTBT×3, ANGX×1, HYLN×1, DNN×1 | $10,285.12 | -0.01 | +0.03 | — | LDI, BTBT, ANGX, HYLN | $10,281.66 | $10,284.88 | DNN×1 |
| 2026-08-20 | +1.12 | $10,281.66 | DNN×1 | $10,284.86 | -0.02 | +105.43 | AG, BHP, HDSN, IAG, KGC, NFGC, DNA, EXK | DNN | $32.35 | $10,364.53 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119 |
| 2026-08-21 | +3.25 | $32.35 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119 | $10,631.13 | +266.60 | -13.20 | BTBT, ORBS | — | $25.49 | $10,617.85 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4 |
| 2026-08-24 | -5.17 | $25.49 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4 | $10,705.93 | +88.08 | -121.00 | — | — | $25.49 | $10,584.93 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4 |
| 2026-08-25 | +1.80 | $25.49 | AG×62, BHP×14, HDSN×222, IAG×65, KGC×43, NFGC×734, DNA×172, EXK×119, BTBT×2, ORBS×4 | $10,460.42 | -124.51 | +41.53 | OCUL, RZLT, HCA, KURA, LIFE, AMTX, AVAH, ETON | AG, BHP, HDSN, IAG, KGC, NFGC, DNA, EXK | $53.58 | $10,450.65 | BTBT×2, ORBS×4, OCUL×118, RZLT×263, HCA×3, KURA×95, LIFE×35, AMTX×686, AVAH×95, ETON×20 |
| 2026-08-26 | +2.02 | $53.58 | BTBT×2, ORBS×4, OCUL×118, RZLT×263, HCA×3, KURA×95, LIFE×35, AMTX×686, AVAH×95, ETON×20 | $10,445.43 | -5.22 | -62.18 | ACRS, TMCI, CRDL | BTBT, ORBS | $40.06 | $10,382.92 | OCUL×118, RZLT×263, HCA×3, KURA×95, LIFE×35, AMTX×686, AVAH×95, ETON×20, ACRS×1, TMCI×1, CRDL×4 |
| 2026-08-27 | — | $40.06 | OCUL×118, RZLT×263, HCA×3, KURA×95, LIFE×35, AMTX×686, AVAH×95, ETON×20, ACRS×1, TMCI×1, CRDL×4 | $10,359.69 | -23.23 | +25.51 | — | — | $40.06 | $10,385.20 | OCUL×118, RZLT×263, HCA×3, KURA×95, LIFE×35, AMTX×686, AVAH×95, ETON×20, ACRS×1, TMCI×1, CRDL×4 |
| 2026-08-28 | +0.75 | $40.06 | OCUL×118, RZLT×263, HCA×3, KURA×95, LIFE×35, AMTX×686, AVAH×95, ETON×20, ACRS×1, TMCI×1, CRDL×4 | $10,409.03 | +23.83 | -310.88 | CRK, GRRR, TTMI, EQ, BTSG, ZYME, ADBT | OCUL, RZLT, HCA, KURA, LIFE, AMTX, AVAH, ETON | $35.81 | $10,050.18 | ACRS×1, TMCI×1, CRDL×4, CRK×101, GRRR×94, TTMI×12, EQ×601, BTSG×24, ZYME×51, ADBT×296 |
| 2026-08-31 | -5.85 | $35.81 | ACRS×1, TMCI×1, CRDL×4, CRK×101, GRRR×94, TTMI×12, EQ×601, BTSG×24, ZYME×51, ADBT×296 | $10,045.85 | -4.33 | -83.94 | — | ACRS, TMCI, CRDL | $53.80 | $9,961.65 | CRK×101, GRRR×94, TTMI×12, EQ×601, BTSG×24, ZYME×51, ADBT×296 |
| 2026-09-01 | -6.30 | $53.80 | CRK×101, GRRR×94, TTMI×12, EQ×601, BTSG×24, ZYME×51, ADBT×296 | $10,008.25 | +46.60 | -277.51 | — | — | $53.80 | $9,730.74 | CRK×101, GRRR×94, TTMI×12, EQ×601, BTSG×24, ZYME×51, ADBT×296 |
| 2026-09-02 | -3.83 | $53.80 | CRK×101, GRRR×94, TTMI×12, EQ×601, BTSG×24, ZYME×51, ADBT×296 | $9,659.22 | -71.52 | +0.00 | — | CRK, GRRR, TTMI, EQ, BTSG, ZYME, ADBT | $9,636.56 | $9,636.56 | — |
| 2026-09-03 | -0.90 | $9,636.56 | — | $9,636.56 | +0.00 | -258.92 | ATRC, HRMY, CABA, RVTY, CRK, ARCT, CRDL, SDGR | — | $71.07 | $9,353.52 | ATRC×22, HRMY×28, CABA×331, RVTY×9, CRK×77, ARCT×71, CRDL×552, SDGR×57 |
| 2026-09-04 | +2.25 | $71.07 | ATRC×22, HRMY×28, CABA×331, RVTY×9, CRK×77, ARCT×71, CRDL×552, SDGR×57 | $9,321.95 | -31.57 | +42.45 | ALEC, BHC, OABI, VIR | — | $23.68 | $9,363.91 | ATRC×22, HRMY×28, CABA×331, RVTY×9, CRK×77, ARCT×71, CRDL×552, SDGR×57, ALEC×5, BHC×2, OABI×2, VIR×1 |
| 2026-09-08 | -11.47 | $23.68 | ATRC×22, HRMY×28, CABA×331, RVTY×9, CRK×77, ARCT×71, CRDL×552, SDGR×57, ALEC×5, BHC×2, OABI×2, VIR×1 | $9,375.65 | +11.74 | -76.79 | — | — | $23.68 | $9,298.86 | ATRC×22, HRMY×28, CABA×331, RVTY×9, CRK×77, ARCT×71, CRDL×552, SDGR×57, ALEC×5, BHC×2, OABI×2, VIR×1 |
| 2026-09-09 | -13.95 | $23.68 | ATRC×22, HRMY×28, CABA×331, RVTY×9, CRK×77, ARCT×71, CRDL×552, SDGR×57, ALEC×5, BHC×2, OABI×2, VIR×1 | $9,255.24 | -43.62 | -2.06 | — | ATRC, HRMY, CABA, RVTY, CRK, ARCT, CRDL, SDGR | $9,186.26 | $9,228.77 | ALEC×5, BHC×2, OABI×2, VIR×1 |
| 2026-09-10 | -13.28 | $9,186.26 | ALEC×5, BHC×2, OABI×2, VIR×1 | $9,228.89 | +0.12 | +0.00 | — | ALEC, BHC, OABI, VIR | $9,228.35 | $9,228.35 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $5,036.64 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 227 | $22.01 | $2.93 | — | $37.44 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.44 | ▲ close $10,677.03 vs 09:30 $10,000.00 (session +682.25) | 16:00 close · cash $37.44 · equity $10,677.03 vs 09:30 $10,000.00 (+677.03; session marks +682.25) · 2 name(s) marked open→close (per-name table). TPG×98 09:30 $50.62 → close $54.62 +391.69; VOR×227 09:30 $22.01 → close $23.29 +290.56 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.44 | ▲ 09:30 equity $10,751.77 vs yday $10,677.03 (+74.74) | 09:30 open · cash $37.44 (unchanged overnight, no fees) · equity $10,751.77 vs prior close $10,677.03 (+74.74) · 2 name(s) re-marked at the open (per-name table). TPG×98 yday $54.62 → 09:30 $55.29 +65.66; VOR×227 yday $23.29 → 09:30 $23.33 +9.08 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 4 | $0.94 | $0.05 | — | $33.65 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3 | $1.50 | $0.05 | — | $29.09 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $24.74 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $20.51 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $4.68 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.51 | ▼ close $10,461.99 vs 09:30 $10,751.77 (session -289.59) | 16:00 close · cash $20.51 · equity $10,461.99 vs 09:30 $10,751.77 (-289.78; session marks -289.59) · 6 name(s) marked open→close (per-name table). TPG×98 09:30 $55.29 → close $53.03 -221.48; VOR×227 09:30 $23.33 → close $23.03 -68.10; LDI×4 09:30 $0.94 → close $0.90 -0.16; BTBT×3 09:30 $1.50 → close $1.57 +0.21; ANGX×1 09:30 $4.31 → close $4.37 +0.06; HYLN×1 09:30 $4.18 → close $4.06 -0.12 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.51 | ▼ 09:30 equity $10,399.63 vs yday $10,461.99 (-62.36) | 09:30 open · cash $20.51 (unchanged overnight, no fees) · equity $10,399.63 vs prior close $10,461.99 (-62.36) · 6 name(s) re-marked at the open (per-name table). TPG×98 yday $53.03 → 09:30 $52.67 -35.28; VOR×227 yday $23.03 → 09:30 $22.91 -27.24; LDI×4 yday $0.90 → 09:30 $0.91 +0.04; BTBT×3 yday $1.57 → 09:30 $1.52 -0.15; ANGX×1 yday $4.37 → 09:30 $4.60 +0.23; HYLN×1 yday $4.06 → 09:30 $4.10 +0.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $17.24 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $4.10 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.24 | ▼ close $10,334.26 vs 09:30 $10,399.63 (session -65.34) | 16:00 close · cash $17.24 · equity $10,334.26 vs 09:30 $10,399.63 (-65.37; session marks -65.34) · 7 name(s) marked open→close (per-name table). TPG×98 09:30 $52.67 → close $51.77 -88.20; VOR×227 09:30 $22.91 → close $23.01 +22.70; LDI×4 09:30 $0.91 → close $0.88 -0.13; BTBT×3 09:30 $1.52 → close $1.60 +0.24; ANGX×1 09:30 $4.60 → close $4.71 +0.11; HYLN×1 09:30 $4.10 → close $4.09 -0.01; DNN×1 09:30 $3.24 → close $3.19 -0.05 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.24 | ▼ 09:30 equity $10,290.79 vs yday $10,334.26 (-43.47) | 09:30 open · cash $17.24 (unchanged overnight, no fees) · equity $10,290.79 vs prior close $10,334.26 (-43.47) · 7 name(s) re-marked at the open (per-name table). TPG×98 yday $51.77 → 09:30 $51.77 +0.00; VOR×227 yday $23.01 → 09:30 $22.82 -43.13; LDI×4 yday $0.88 → 09:30 $0.87 -0.02; BTBT×3 yday $1.60 → 09:30 $1.54 -0.18; ANGX×1 yday $4.71 → 09:30 $4.79 +0.08; HYLN×1 yday $4.09 → 09:30 $3.95 -0.14; DNN×1 yday $3.19 → 09:30 $3.11 -0.08 | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 98 | $51.77 | $2.34 | $+107.76 | $5,088.36 | ▲ +107.76 after sell → book $10,288.45; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 227 | $22.82 | $3.01 | $+177.93 | $10,265.49 | ▲ +177.93 after sell → book $10,285.44; vs 09:30 mark -3.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,265.49 | ▼ close $10,285.13 vs 09:30 $10,290.79 (session -0.31) | 16:00 close · cash $10,265.49 · equity $10,285.13 vs 09:30 $10,290.79 (-5.66; session marks -0.31) · 5 name(s) marked open→close (per-name table). LDI×4 09:30 $0.87 → close $0.86 -0.05; BTBT×3 09:30 $1.54 → close $1.45 -0.27; ANGX×1 09:30 $4.79 → close $4.85 +0.06; HYLN×1 09:30 $3.95 → close $3.86 -0.09; DNN×1 09:30 $3.11 → close $3.15 +0.04 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,265.49 | ▼ 09:30 equity $10,285.12 vs yday $10,285.13 (-0.01) | 09:30 open · cash $10,265.49 (unchanged overnight, no fees) · equity $10,285.12 vs prior close $10,285.13 (-0.01) · 5 name(s) re-marked at the open (per-name table). LDI×4 yday $0.86 → 09:30 $0.88 +0.09; BTBT×3 yday $1.45 → 09:30 $1.42 -0.09; ANGX×1 yday $4.85 → 09:30 $4.79 -0.06; HYLN×1 yday $3.86 → 09:30 $3.87 +0.01; DNN×1 yday $3.15 → 09:30 $3.19 +0.04 | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 4 | $0.88 | $0.07 | $-0.34 | $10,268.94 | ▼ -0.34 after sell → book $10,285.05; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 3 | $1.42 | $0.07 | $-0.37 | $10,273.13 | ▼ -0.37 after sell → book $10,284.98; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 1 | $4.79 | $0.07 | $+0.36 | $10,277.85 | ▲ +0.36 after sell → book $10,284.91; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 1 | $3.87 | $0.06 | $-0.42 | $10,281.66 | ▼ -0.42 after sell → book $10,284.85; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,281.66 | ▲ close $10,284.88 vs 09:30 $10,285.12 (session +0.03) | 16:00 close · cash $10,281.66 · equity $10,284.88 vs 09:30 $10,285.12 (-0.24; session marks +0.03) · 1 name(s) marked open→close (per-name table). DNN×1 09:30 $3.19 → close $3.22 +0.03 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,281.66 | ▼ 09:30 equity $10,284.86 vs yday $10,284.88 (-0.02) | 09:30 open · cash $10,281.66 (unchanged overnight, no fees) · equity $10,284.86 vs prior close $10,284.88 (-0.02) · 1 name(s) re-marked at the open (per-name table). DNN×1 yday $3.22 → 09:30 $3.20 -0.02 | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 1 | $3.20 | $0.06 | $-0.13 | $10,284.80 | ▼ -0.13 after sell → book $10,284.80; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,008.53 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1285.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,732.35 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1285.60 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 222 | $5.77 | $2.86 | — | $6,448.55 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1285.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $5,170.41 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1285.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $3,894.21 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1285.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 734 | $1.75 | $9.47 | — | $2,600.24 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1285.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 172 | $7.45 | $2.51 | — | $1,316.33 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1285.60 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 119 | $10.77 | $2.35 | — | $32.35 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1285.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.35 | ▲ close $10,364.53 vs 09:30 $10,284.86 (session +105.43) | 16:00 close · cash $32.35 · equity $10,364.53 vs 09:30 $10,284.86 (+79.67; session marks +105.43) · 8 name(s) marked open→close (per-name table). AG×62 09:30 $20.55 → close $21.19 +39.68; BHP×14 09:30 $91.01 → close $93.63 +36.68; HDSN×222 09:30 $5.77 → close $5.57 -44.40; IAG×65 09:30 $19.63 → close $20.50 +56.55; KGC×43 09:30 $29.63 → close $31.43 +77.40; NFGC×734 09:30 $1.75 → close $1.75 +0.00; DNA×172 09:30 $7.45 → close $6.96 -84.28; EXK×119 09:30 $10.77 → close $10.97 +23.80 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.35 | ▲ 09:30 equity $10,631.13 vs yday $10,364.53 (+266.60) | 09:30 open · cash $32.35 (unchanged overnight, no fees) · equity $10,631.13 vs prior close $10,364.53 (+266.60) · 8 name(s) re-marked at the open (per-name table). AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; HDSN×222 yday $5.57 → 09:30 $5.67 +22.20; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×734 yday $1.75 → 09:30 $1.79 +29.36; DNA×172 yday $6.96 → 09:30 $7.09 +22.36; EXK×119 yday $10.97 → 09:30 $11.34 +44.03 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 2 | $1.66 | $0.04 | — | $29.00 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $4.04 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 4 | $0.86 | $0.05 | — | $25.49 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $4.04 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.49 | ▼ close $10,617.85 vs 09:30 $10,631.13 (session -13.20) | 16:00 close · cash $25.49 · equity $10,617.85 vs 09:30 $10,631.13 (-13.28; session marks -13.20) · 10 name(s) marked open→close (per-name table). AG×62 09:30 $21.90 → close $21.09 -50.22; BHP×14 09:30 $95.72 → close $97.03 +18.34; HDSN×222 09:30 $5.67 → close $5.63 -8.88; IAG×65 09:30 $21.17 → close $21.14 -1.95; KGC×43 09:30 $32.17 → close $32.76 +25.37; NFGC×734 09:30 $1.79 → close $1.84 +36.70; DNA×172 09:30 $7.09 → close $7.40 +53.32; EXK×119 09:30 $11.34 → close $10.62 -85.68; BTBT×2 09:30 $1.66 → close $1.53 -0.26; ORBS×4 09:30 $0.86 → close $0.88 +0.06 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.49 | ▲ 09:30 equity $10,705.93 vs yday $10,617.85 (+88.08) | 09:30 open · cash $25.49 (unchanged overnight, no fees) · equity $10,705.93 vs prior close $10,617.85 (+88.08) · 10 name(s) re-marked at the open (per-name table). AG×62 yday $21.09 → 09:30 $21.30 +13.02; BHP×14 yday $97.03 → 09:30 $97.31 +3.92; HDSN×222 yday $5.63 → 09:30 $5.69 +13.32; IAG×65 yday $21.14 → 09:30 $21.38 +15.60; KGC×43 yday $32.76 → 09:30 $33.03 +11.61; NFGC×734 yday $1.84 → 09:30 $1.86 +14.68; DNA×172 yday $7.40 → 09:30 $7.25 -25.80; EXK×119 yday $10.62 → 09:30 $10.97 +41.65; BTBT×2 yday $1.53 → 09:30 $1.55 +0.04; ORBS×4 yday $0.88 → 09:30 $0.89 +0.04 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.49 | ▼ close $10,584.93 vs 09:30 $10,705.93 (session -121.00) | 16:00 close · cash $25.49 · equity $10,584.93 vs 09:30 $10,705.93 (-121.00; session marks -121.00) · 10 name(s) marked open→close (per-name table). AG×62 09:30 $21.30 → close $20.83 -29.14; BHP×14 09:30 $97.31 → close $97.13 -2.52; HDSN×222 09:30 $5.69 → close $5.52 -37.74; IAG×65 09:30 $21.38 → close $21.80 +27.30; KGC×43 09:30 $33.03 → close $32.98 -2.15; NFGC×734 09:30 $1.86 → close $1.90 +29.36; DNA×172 09:30 $7.25 → close $6.78 -80.84; EXK×119 09:30 $10.97 → close $10.76 -24.99; BTBT×2 09:30 $1.55 → close $1.51 -0.08; ORBS×4 09:30 $0.89 → close $0.84 -0.20 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.49 | ▼ 09:30 equity $10,460.42 vs yday $10,584.93 (-124.51) | 09:30 open · cash $25.49 (unchanged overnight, no fees) · equity $10,460.42 vs prior close $10,584.93 (-124.51) · 10 name(s) re-marked at the open (per-name table). AG×62 yday $20.83 → 09:30 $20.32 -31.62; BHP×14 yday $97.13 → 09:30 $95.86 -17.78; HDSN×222 yday $5.52 → 09:30 $5.53 +2.22; IAG×65 yday $21.80 → 09:30 $21.21 -38.35; KGC×43 yday $32.98 → 09:30 $32.32 -28.38; NFGC×734 yday $1.90 → 09:30 $1.90 +0.00; DNA×172 yday $6.78 → 09:30 $6.94 +27.52; EXK×119 yday $10.76 → 09:30 $10.44 -38.08; BTBT×2 yday $1.51 → 09:30 $1.51 +0.00; ORBS×4 yday $0.84 → 09:30 $0.83 -0.04 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $1,283.14 | ▼ -18.63 after sell → book $10,458.23; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.86 | $2.05 | $+63.82 | $2,623.12 | ▲ +63.82 after sell → book $10,456.17; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 222 | $5.53 | $2.91 | $-59.05 | $3,847.87 | ▼ -59.05 after sell → book $10,453.26; vs 09:30 mark -2.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $5,224.32 | ▲ +98.31 after sell → book $10,451.06; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $6,611.94 | ▲ +111.41 after sell → book $10,448.92; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 734 | $1.90 | $9.60 | $+91.03 | $7,996.93 | ▲ +91.03 after sell → book $10,439.31; vs 09:30 mark -9.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 172 | $6.94 | $2.54 | $-92.77 | $9,188.07 | ▼ -92.77 after sell → book $10,436.77; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 119 | $10.44 | $2.38 | $-43.99 | $10,428.05 | ▼ -43.99 after sell → book $10,434.39; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 118 | $10.98 | $2.34 | — | $9,130.07 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+1.2; leftover $1303.51 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 263 | $4.94 | $3.39 | — | $7,827.46 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+7.1; leftover $1303.51 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $6,544.55 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+6.0; leftover $1303.51 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 95 | $13.59 | $2.27 | — | $5,251.22 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1303.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 35 | $36.96 | $2.10 | — | $3,955.53 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1303.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 686 | $1.90 | $8.85 | — | $2,643.28 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ⚪; ret5=+5.0; leftover $1303.51 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 95 | $13.62 | $2.27 | — | $1,346.63 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1303.51 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 20 | $64.55 | $2.05 | — | $53.58 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.4; leftover $1303.51 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.58 | ▲ close $10,450.65 vs 09:30 $10,460.42 (session +41.53) | 16:00 close · cash $53.58 · equity $10,450.65 vs 09:30 $10,460.42 (-9.77; session marks +41.53) · 10 name(s) marked open→close (per-name table). BTBT×2 09:30 $1.51 → close $1.58 +0.14; ORBS×4 09:30 $0.83 → close $0.80 -0.12; OCUL×118 09:30 $10.98 → close $10.88 -11.80; RZLT×263 09:30 $4.94 → close $5.01 +18.41; HCA×3 09:30 $426.97 → close $428.76 +5.37; KURA×95 09:30 $13.59 → close $13.59 +0.00; LIFE×35 09:30 $36.96 → close $38.56 +56.00; AMTX×686 09:30 $1.90 → close $1.91 +6.86; AVAH×95 09:30 $13.62 → close $13.59 -3.33; ETON×20 09:30 $64.55 → close $63.05 -30.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.58 | ▼ 09:30 equity $10,445.43 vs yday $10,450.65 (-5.22) | 09:30 open · cash $53.58 (unchanged overnight, no fees) · equity $10,445.43 vs prior close $10,450.65 (-5.22) · 10 name(s) re-marked at the open (per-name table). BTBT×2 yday $1.58 → 09:30 $1.53 -0.10; ORBS×4 yday $0.80 → 09:30 $0.80 -0.02; OCUL×118 yday $10.88 → 09:30 $10.79 -10.62; RZLT×263 yday $5.01 → 09:30 $5.01 +0.00; HCA×3 yday $428.76 → 09:30 $427.50 -3.78; KURA×95 yday $13.59 → 09:30 $13.63 +3.80; LIFE×35 yday $38.56 → 09:30 $38.24 -11.20; AMTX×686 yday $1.91 → 09:30 $1.91 +0.00; AVAH×95 yday $13.59 → 09:30 $13.65 +5.70; ETON×20 yday $63.05 → 09:30 $63.60 +11.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 2 | $1.53 | $0.06 | $-0.36 | $56.58 | ▼ -0.36 after sell → book $10,445.38; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ORBS` | 4 | $0.80 | $0.06 | $-0.38 | $59.70 | ▼ -0.38 after sell → book $10,445.31; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 1 | $6.53 | $0.07 | — | $53.10 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.6; leftover $8.53 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TMCI` | 1 | $4.78 | $0.05 | — | $48.27 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+8.1; leftover $8.53 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CRDL` | 4 | $2.03 | $0.09 | — | $40.06 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+5.5; leftover $8.53 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.06 | ▼ close $10,382.92 vs 09:30 $10,445.43 (session -62.18) | 16:00 close · cash $40.06 · equity $10,382.92 vs 09:30 $10,445.43 (-62.51; session marks -62.18) · 11 name(s) marked open→close (per-name table). OCUL×118 09:30 $10.79 → close $10.77 -2.36; RZLT×263 09:30 $5.01 → close $5.04 +7.89; HCA×3 09:30 $427.50 → close $427.16 -1.02; KURA×95 09:30 $13.63 → close $13.06 -54.15; LIFE×35 09:30 $38.24 → close $39.11 +30.45; AMTX×686 09:30 $1.91 → close $1.88 -20.58; AVAH×95 09:30 $13.65 → close $13.62 -2.85; ETON×20 09:30 $63.60 → close $62.62 -19.60; ACRS×1 09:30 $6.53 → close $6.19 -0.34; TMCI×1 09:30 $4.78 → close $4.72 -0.06; CRDL×4 09:30 $2.03 → close $2.14 +0.44 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.06 | ▼ 09:30 equity $10,359.69 vs yday $10,382.92 (-23.23) | 09:30 open · cash $40.06 (unchanged overnight, no fees) · equity $10,359.69 vs prior close $10,382.92 (-23.23) · 11 name(s) re-marked at the open (per-name table). OCUL×118 yday $10.77 → 09:30 $10.63 -16.52; RZLT×263 yday $5.04 → 09:30 $5.07 +7.89; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; KURA×95 yday $13.06 → 09:30 $12.98 -7.60; LIFE×35 yday $39.11 → 09:30 $39.40 +10.15; AMTX×686 yday $1.88 → 09:30 $1.87 -6.86; AVAH×95 yday $13.62 → 09:30 $13.62 +0.00; ETON×20 yday $62.62 → 09:30 $62.50 -2.40; ACRS×1 yday $6.19 → 09:30 $6.15 -0.04; TMCI×1 yday $4.72 → 09:30 $4.72 +0.00; CRDL×4 yday $2.14 → 09:30 $2.09 -0.20 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.06 | ▲ close $10,385.20 vs 09:30 $10,359.69 (session +25.51) | 16:00 close · cash $40.06 · equity $10,385.20 vs 09:30 $10,359.69 (+25.51; session marks +25.51) · 11 name(s) marked open→close (per-name table). OCUL×118 09:30 $10.63 → close $10.82 +22.42; RZLT×263 09:30 $5.07 → close $4.98 -23.67; HCA×3 09:30 $424.61 → close $419.34 -15.81; KURA×95 09:30 $12.98 → close $13.18 +19.00; LIFE×35 09:30 $39.40 → close $39.44 +1.40; AMTX×686 09:30 $1.87 → close $1.87 +0.00; AVAH×95 09:30 $13.62 → close $13.82 +19.00; ETON×20 09:30 $62.50 → close $62.67 +3.40; ACRS×1 09:30 $6.15 → close $6.16 +0.01; TMCI×1 09:30 $4.72 → close $4.60 -0.12; CRDL×4 09:30 $2.09 → close $2.06 -0.12 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.06 | ▲ 09:30 equity $10,409.03 vs yday $10,385.20 (+23.83) | 09:30 open · cash $40.06 (unchanged overnight, no fees) · equity $10,409.03 vs prior close $10,385.20 (+23.83) · 11 name(s) re-marked at the open (per-name table). OCUL×118 yday $10.82 → 09:30 $10.97 +17.70; RZLT×263 yday $4.98 → 09:30 $4.95 -7.89; HCA×3 yday $419.34 → 09:30 $423.76 +13.26; KURA×95 yday $13.18 → 09:30 $13.05 -12.35; LIFE×35 yday $39.44 → 09:30 $39.60 +5.60; AMTX×686 yday $1.87 → 09:30 $1.89 +13.72; AVAH×95 yday $13.82 → 09:30 $13.90 +7.60; ETON×20 yday $62.67 → 09:30 $61.98 -13.80; ACRS×1 yday $6.16 → 09:30 $6.10 -0.06; TMCI×1 yday $4.60 → 09:30 $4.65 +0.05; CRDL×4 yday $2.06 → 09:30 $2.06 +0.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 118 | $10.97 | $2.37 | $-5.90 | $1,332.15 | ▼ -5.90 after sell → book $10,406.66; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 263 | $4.95 | $3.45 | $-4.21 | $2,630.55 | ▼ -4.21 after sell → book $10,403.21; vs 09:30 mark -3.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $423.76 | $2.02 | $-13.65 | $3,899.81 | ▼ -13.65 after sell → book $10,401.19; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 95 | $13.05 | $2.30 | $-55.88 | $5,137.26 | ▼ -55.88 after sell → book $10,398.89; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 35 | $39.60 | $2.12 | $+88.19 | $6,521.14 | ▲ +88.19 after sell → book $10,396.77; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AMTX` | 686 | $1.89 | $8.97 | $-24.68 | $7,808.71 | ▼ -24.68 after sell → book $10,387.80; vs 09:30 mark -8.97 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AVAH` | 95 | $13.90 | $2.30 | $+21.55 | $9,126.91 | ▲ +21.55 after sell → book $10,385.50; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ETON` | 20 | $61.98 | $2.07 | $-55.52 | $10,364.44 | ▼ -55.52 after sell → book $10,383.43; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 101 | $14.63 | $2.29 | — | $8,884.51 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+5.8; leftover $1480.63 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 94 | $15.66 | $2.27 | — | $7,410.20 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1480.63 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 12 | $122.81 | $2.03 | — | $5,934.46 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1480.63 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 601 | $2.46 | $7.75 | — | $4,448.24 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+7.9; leftover $1480.63 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BTSG` | 24 | $60.54 | $2.06 | — | $2,993.22 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+2.3; leftover $1480.63 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 51 | $28.91 | $2.14 | — | $1,516.67 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+9.2; leftover $1480.63 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ADBT` | 296 | $4.99 | $3.82 | — | $35.81 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+0.0; leftover $1480.63 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.81 | ▼ close $10,050.18 vs 09:30 $10,409.03 (session -310.88) | 16:00 close · cash $35.81 · equity $10,050.18 vs 09:30 $10,409.03 (-358.85; session marks -310.88) · 10 name(s) marked open→close (per-name table). ACRS×1 09:30 $6.10 → close $6.01 -0.09; TMCI×1 09:30 $4.65 → close $4.65 +0.00; CRDL×4 09:30 $2.06 → close $1.94 -0.48; CRK×101 09:30 $14.63 → close $14.29 -34.34; GRRR×94 09:30 $15.66 → close $14.41 -117.50; TTMI×12 09:30 $122.81 → close $118.65 -49.92; EQ×601 09:30 $2.46 → close $2.39 -42.07; BTSG×24 09:30 $60.54 → close $59.13 -33.84; ZYME×51 09:30 $28.91 → close $28.27 -32.64; ADBT×296 09:30 $4.99 → close $4.99 +0.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.81 | ▼ 09:30 equity $10,045.85 vs yday $10,050.18 (-4.33) | 09:30 open · cash $35.81 (unchanged overnight, no fees) · equity $10,045.85 vs prior close $10,050.18 (-4.33) · 10 name(s) re-marked at the open (per-name table). ACRS×1 yday $6.01 → 09:30 $5.97 -0.04; TMCI×1 yday $4.65 → 09:30 $4.60 -0.05; CRDL×4 yday $1.94 → 09:30 $1.92 -0.08; CRK×101 yday $14.29 → 09:30 $14.54 +25.25; GRRR×94 yday $14.41 → 09:30 $14.44 +2.82; TTMI×12 yday $118.65 → 09:30 $118.83 +2.16; EQ×601 yday $2.39 → 09:30 $2.39 +0.00; BTSG×24 yday $59.13 → 09:30 $58.76 -8.88; ZYME×51 yday $28.27 → 09:30 $28.06 -10.71; ADBT×296 yday $4.99 → 09:30 $4.94 -14.80 | — |
| 2026-08-31 09:30 ET | **SELL** | `ACRS` | 1 | $5.97 | $0.08 | $-0.71 | $41.70 | ▼ -0.71 after sell → book $10,045.77; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TMCI` | 1 | $4.60 | $0.07 | $-0.30 | $46.23 | ▼ -0.30 after sell → book $10,045.70; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRDL` | 4 | $1.92 | $0.11 | $-0.64 | $53.80 | ▼ -0.64 after sell → book $10,045.59; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.80 | ▼ close $9,961.65 vs 09:30 $10,045.85 (session -83.94) | 16:00 close · cash $53.80 · equity $9,961.65 vs 09:30 $10,045.85 (-84.20; session marks -83.94) · 7 name(s) marked open→close (per-name table). CRK×101 09:30 $14.54 → close $14.43 -11.11; GRRR×94 09:30 $14.44 → close $15.04 +56.40; TTMI×12 09:30 $118.83 → close $118.92 +1.08; EQ×601 09:30 $2.39 → close $2.27 -72.12; BTSG×24 09:30 $58.76 → close $58.40 -8.64; ZYME×51 09:30 $28.06 → close $29.41 +68.85; ADBT×296 09:30 $4.94 → close $4.54 -118.40 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.80 | ▲ 09:30 equity $10,008.25 vs yday $9,961.65 (+46.60) | 09:30 open · cash $53.80 (unchanged overnight, no fees) · equity $10,008.25 vs prior close $9,961.65 (+46.60) · 7 name(s) re-marked at the open (per-name table). CRK×101 yday $14.43 → 09:30 $15.82 +140.39; GRRR×94 yday $15.04 → 09:30 $14.75 -27.26; TTMI×12 yday $118.92 → 09:30 $116.68 -26.88; EQ×601 yday $2.27 → 09:30 $2.25 -12.02; BTSG×24 yday $58.40 → 09:30 $58.55 +3.60; ZYME×51 yday $29.41 → 09:30 $29.32 -4.59; ADBT×296 yday $4.54 → 09:30 $4.45 -26.64 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.80 | ▼ close $9,730.74 vs 09:30 $10,008.25 (session -277.51) | 16:00 close · cash $53.80 · equity $9,730.74 vs 09:30 $10,008.25 (-277.51; session marks -277.51) · 7 name(s) marked open→close (per-name table). CRK×101 09:30 $15.82 → close $16.02 +20.20; GRRR×94 09:30 $14.75 → close $14.09 -62.04; TTMI×12 09:30 $116.68 → close $115.33 -16.20; EQ×601 09:30 $2.25 → close $2.21 -24.04; BTSG×24 09:30 $58.55 → close $59.16 +14.64; ZYME×51 09:30 $29.32 → close $29.67 +17.85; ADBT×296 09:30 $4.45 → close $3.68 -227.92 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.80 | ▼ 09:30 equity $9,659.22 vs yday $9,730.74 (-71.52) | 09:30 open · cash $53.80 (unchanged overnight, no fees) · equity $9,659.22 vs prior close $9,730.74 (-71.52) · 7 name(s) re-marked at the open (per-name table). CRK×101 yday $16.02 → 09:30 $15.70 -32.32; GRRR×94 yday $14.09 → 09:30 $13.92 -15.98; TTMI×12 yday $115.33 → 09:30 $114.22 -13.32; EQ×601 yday $2.21 → 09:30 $2.20 -6.01; BTSG×24 yday $59.16 → 09:30 $59.16 +0.00; ZYME×51 yday $29.67 → 09:30 $30.00 +16.83; ADBT×296 yday $3.68 → 09:30 $3.61 -20.72 | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 101 | $15.70 | $2.32 | $+103.45 | $1,637.18 | ▲ +103.45 after sell → book $9,656.90; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 94 | $13.92 | $2.30 | $-168.13 | $2,943.36 | ▼ -168.13 after sell → book $9,654.60; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 12 | $114.22 | $2.05 | $-107.15 | $4,311.95 | ▼ -107.15 after sell → book $9,652.55; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `EQ` | 601 | $2.20 | $7.86 | $-171.88 | $5,626.29 | ▼ -171.88 after sell → book $9,644.69; vs 09:30 mark -7.86 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BTSG` | 24 | $59.16 | $2.08 | $-37.27 | $7,044.05 | ▼ -37.27 after sell → book $9,642.61; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 51 | $30.00 | $2.17 | $+51.28 | $8,571.88 | ▲ +51.28 after sell → book $9,640.44; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADBT` | 296 | $3.61 | $3.88 | $-416.18 | $9,636.56 | ▼ -416.18 after sell → book $9,636.56; vs 09:30 mark -3.88 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,636.56 | ▲ close $9,636.56 vs 09:30 $9,659.22 (session +0.00) | 16:00 close · cash $9,636.56 · no lots left · equity $9,636.56. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,636.56 | ▲ 09:30 equity $9,636.56 vs yday $9,636.56 (+0.00) | 09:30 open · cash $9,636.56 · no holdings · equity $9,636.56 vs prior close $9,636.56 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 22 | $52.88 | $2.06 | — | $8,471.15 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1204.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $7,267.03 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1204.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 331 | $3.63 | $4.27 | — | $6,061.23 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1204.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $4,867.17 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1204.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 77 | $15.45 | $2.22 | — | $3,675.29 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1204.57 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 71 | $16.77 | $2.20 | — | $2,482.42 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1204.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 552 | $2.18 | $7.12 | — | $1,271.94 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1204.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 57 | $21.03 | $2.16 | — | $71.07 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1204.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.07 | ▼ close $9,353.52 vs 09:30 $9,636.56 (session -258.92) | 16:00 close · cash $71.07 · equity $9,353.52 vs 09:30 $9,636.56 (-283.04; session marks -258.92) · 8 name(s) marked open→close (per-name table). ATRC×22 09:30 $52.88 → close $52.46 -9.24; HRMY×28 09:30 $42.93 → close $41.86 -29.96; CABA×331 09:30 $3.63 → close $3.48 -49.65; RVTY×9 09:30 $132.45 → close $130.63 -16.38; CRK×77 09:30 $15.45 → close $14.95 -38.50; ARCT×71 09:30 $16.77 → close $15.56 -85.91; CRDL×552 09:30 $2.18 → close $2.16 -11.04; SDGR×57 09:30 $21.03 → close $20.71 -18.24 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.07 | ▼ 09:30 equity $9,321.95 vs yday $9,353.52 (-31.57) | 09:30 open · cash $71.07 (unchanged overnight, no fees) · equity $9,321.95 vs prior close $9,353.52 (-31.57) · 8 name(s) re-marked at the open (per-name table). ATRC×22 yday $52.46 → 09:30 $52.03 -9.46; HRMY×28 yday $41.86 → 09:30 $41.50 -10.08; CABA×331 yday $3.48 → 09:30 $3.46 -6.62; RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; CRK×77 yday $14.95 → 09:30 $15.00 +3.85; ARCT×71 yday $15.56 → 09:30 $15.61 +3.55; CRDL×552 yday $2.16 → 09:30 $2.16 +0.00; SDGR×57 yday $20.71 → 09:30 $20.58 -7.41 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 5 | $2.52 | $0.14 | — | $58.33 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $14.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 2 | $6.71 | $0.14 | — | $44.77 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $14.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 2 | $4.78 | $0.10 | — | $35.11 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $14.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $23.68 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $14.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.68 | ▲ close $9,363.91 vs 09:30 $9,321.95 (session +42.45) | 16:00 close · cash $23.68 · equity $9,363.91 vs 09:30 $9,321.95 (+41.96; session marks +42.45) · 12 name(s) marked open→close (per-name table). ATRC×22 09:30 $52.03 → close $51.52 -11.22; HRMY×28 09:30 $41.50 → close $42.25 +21.00; CABA×331 09:30 $3.46 → close $3.47 +3.31; RVTY×9 09:30 $130.03 → close $130.22 +1.71; CRK×77 09:30 $15.00 → close $15.26 +20.02; ARCT×71 09:30 $15.61 → close $15.82 +14.91; CRDL×552 09:30 $2.16 → close $2.20 +22.08; SDGR×57 09:30 $20.58 → close $20.09 -27.93; ALEC×5 09:30 $2.52 → close $2.46 -0.30; BHC×2 09:30 $6.71 → close $6.56 -0.30; OABI×2 09:30 $4.78 → close $4.33 -0.90; VIR×1 09:30 $11.31 → close $11.38 +0.07 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.68 | ▲ 09:30 equity $9,375.65 vs yday $9,363.91 (+11.74) | 09:30 open · cash $23.68 (unchanged overnight, no fees) · equity $9,375.65 vs prior close $9,363.91 (+11.74) · 12 name(s) re-marked at the open (per-name table). ATRC×22 yday $51.52 → 09:30 $54.31 +61.38; HRMY×28 yday $42.25 → 09:30 $42.20 -1.40; CABA×331 yday $3.47 → 09:30 $3.43 -13.24; RVTY×9 yday $130.22 → 09:30 $128.50 -15.48; CRK×77 yday $15.26 → 09:30 $15.50 +18.48; ARCT×71 yday $15.82 → 09:30 $15.47 -24.85; CRDL×552 yday $2.20 → 09:30 $2.20 +0.00; SDGR×57 yday $20.09 → 09:30 $19.87 -12.54; ALEC×5 yday $2.46 → 09:30 $2.38 -0.40; BHC×2 yday $6.56 → 09:30 $6.57 +0.02; OABI×2 yday $4.33 → 09:30 $4.30 -0.06; VIR×1 yday $11.38 → 09:30 $11.22 -0.16 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.68 | ▼ close $9,298.86 vs 09:30 $9,375.65 (session -76.79) | 16:00 close · cash $23.68 · equity $9,298.86 vs 09:30 $9,375.65 (-76.79; session marks -76.79) · 12 name(s) marked open→close (per-name table). ATRC×22 09:30 $54.31 → close $53.73 -12.76; HRMY×28 09:30 $42.20 → close $42.07 -3.64; CABA×331 09:30 $3.43 → close $3.27 -52.96; RVTY×9 09:30 $128.50 → close $127.08 -12.78; CRK×77 09:30 $15.50 → close $15.16 -26.18; ARCT×71 09:30 $15.47 → close $15.63 +11.36; CRDL×552 09:30 $2.20 → close $2.22 +11.04; SDGR×57 09:30 $19.87 → close $20.03 +9.12; ALEC×5 09:30 $2.38 → close $2.47 +0.45; BHC×2 09:30 $6.57 → close $6.43 -0.28; OABI×2 09:30 $4.30 → close $4.24 -0.12; VIR×1 09:30 $11.22 → close $11.18 -0.04 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.68 | ▼ 09:30 equity $9,255.24 vs yday $9,298.86 (-43.62) | 09:30 open · cash $23.68 (unchanged overnight, no fees) · equity $9,255.24 vs prior close $9,298.86 (-43.62) · 12 name(s) re-marked at the open (per-name table). ATRC×22 yday $53.73 → 09:30 $53.16 -12.54; HRMY×28 yday $42.07 → 09:30 $42.01 -1.68; CABA×331 yday $3.27 → 09:30 $3.28 +3.31; RVTY×9 yday $127.08 → 09:30 $125.77 -11.79; CRK×77 yday $15.16 → 09:30 $15.16 +0.00; ARCT×71 yday $15.63 → 09:30 $15.46 -12.07; CRDL×552 yday $2.22 → 09:30 $2.22 +0.00; SDGR×57 yday $20.03 → 09:30 $19.88 -8.55; ALEC×5 yday $2.47 → 09:30 $2.47 +0.00; BHC×2 yday $6.43 → 09:30 $6.38 -0.10; OABI×2 yday $4.24 → 09:30 $4.21 -0.06; VIR×1 yday $11.18 → 09:30 $11.04 -0.14 | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 22 | $53.16 | $2.08 | $+2.03 | $1,191.12 | ▲ +2.03 after sell → book $9,253.16; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 28 | $42.01 | $2.09 | $-29.93 | $2,365.31 | ▼ -29.93 after sell → book $9,251.07; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 331 | $3.28 | $4.33 | $-124.45 | $3,446.66 | ▼ -124.45 after sell → book $9,246.74; vs 09:30 mark -4.33 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $4,576.55 | ▼ -64.17 after sell → book $9,244.70; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 77 | $15.16 | $2.24 | $-26.79 | $5,741.63 | ▼ -26.79 after sell → book $9,242.46; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 71 | $15.46 | $2.22 | $-97.44 | $6,837.06 | ▼ -97.44 after sell → book $9,240.23; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 552 | $2.22 | $7.22 | $+7.74 | $8,055.28 | ▲ +7.74 after sell → book $9,233.01; vs 09:30 mark -7.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SDGR` | 57 | $19.88 | $2.18 | $-69.89 | $9,186.26 | ▼ -69.89 after sell → book $9,230.83; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,186.26 | ▼ close $9,228.77 vs 09:30 $9,255.24 (session -2.06) | 16:00 close · cash $9,186.26 · equity $9,228.77 vs 09:30 $9,255.24 (-26.47; session marks -2.06) · 4 name(s) marked open→close (per-name table). ALEC×5 09:30 $2.47 → close $2.27 -1.00; BHC×2 09:30 $6.38 → close $6.16 -0.44; OABI×2 09:30 $4.21 → close $4.01 -0.39; VIR×1 09:30 $11.04 → close $10.81 -0.23 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,186.26 | ▲ 09:30 equity $9,228.89 vs yday $9,228.77 (+0.12) | 09:30 open · cash $9,186.26 (unchanged overnight, no fees) · equity $9,228.89 vs prior close $9,228.77 (+0.12) · 4 name(s) re-marked at the open (per-name table). ALEC×5 yday $2.27 → 09:30 $2.27 +0.00; BHC×2 yday $6.16 → 09:30 $6.19 +0.06; OABI×2 yday $4.01 → 09:30 $4.01 -0.01; VIR×1 yday $10.81 → 09:30 $10.88 +0.07 | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 5 | $2.27 | $0.15 | $-1.54 | $9,197.46 | ▼ -1.54 after sell → book $9,228.74; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 2 | $6.19 | $0.15 | $-1.33 | $9,209.69 | ▼ -1.33 after sell → book $9,228.59; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 2 | $4.01 | $0.11 | $-1.75 | $9,217.60 | ▼ -1.75 after sell → book $9,228.48; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 1 | $10.88 | $0.13 | $-0.68 | $9,228.35 | ▼ -0.68 after sell → book $9,228.35; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,228.35 | ▲ close $9,228.35 vs 09:30 $9,228.89 (session +0.00) | 16:00 close · cash $9,228.35 · no lots left · equity $9,228.35. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 4.68 < 1 share @ 359.83 |
| 2026-08-14 | `SLG` | cash | leftover split 4.68 < 1 share @ 57.61 |
| 2026-08-14 | `WDC` | cash | leftover split 4.68 < 1 share @ 503.50 |
| 2026-08-14 | `ADUR` | cash | leftover split 4.68 < 1 share @ 16.50 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 4.10 < 1 share @ 46.18 |
| 2026-08-17 | `OCC` | cash | leftover split 4.10 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 4.10 < 1 share @ 16.20 |
| 2026-08-17 | `NEWP` | cash | leftover split 4.10 < 1 share @ 6.94 |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MXL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 4.04 < 1 share @ 59.72 |
| 2026-08-21 | `CF` | cash | leftover split 4.04 < 1 share @ 127.43 |
| 2026-08-21 | `EMBC` | cash | leftover split 4.04 < 1 share @ 5.43 |
| 2026-08-21 | `TXG` | cash | leftover split 4.04 < 1 share @ 64.39 |
| 2026-08-21 | `DXYZ` | cash | leftover split 4.04 < 1 share @ 34.89 |
| 2026-08-21 | `BEKE` | cash | leftover split 4.04 < 1 share @ 17.93 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `OCUL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HCA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ETON` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `INSP` | cash | leftover split 8.53 < 1 share @ 60.07 |
| 2026-08-26 | `CRMD` | cash | leftover split 8.53 < 1 share @ 8.60 |
| 2026-08-26 | `SENS` | cash | leftover split 8.53 < 1 share @ 9.48 |
| 2026-08-26 | `BE` | cash | leftover split 8.53 < 1 share @ 213.94 |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ETON` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `TMCI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 5.01 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 5.01 < 1 share @ 14.42 |
| 2026-08-27 | `MOS` | cash | leftover split 5.01 < 1 share @ 24.00 |
| 2026-08-27 | `ANET` | cash | leftover split 5.01 < 1 share @ 205.90 |
| 2026-08-27 | `DLO` | cash | leftover split 5.01 < 1 share @ 15.33 |
| 2026-08-27 | `GEN` | cash | leftover split 5.01 < 1 share @ 29.83 |
| 2026-08-27 | `MRVL` | cash | leftover split 5.01 < 1 share @ 253.44 |
| 2026-08-27 | `NUE` | cash | leftover split 5.01 < 1 share @ 252.00 |
| 2026-08-28 | `ACRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TMCI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `EQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `EQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 14.21 < 1 share @ 263.36 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SDGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OBE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HELP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
