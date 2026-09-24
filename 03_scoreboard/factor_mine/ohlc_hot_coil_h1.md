# Factor mine action — `ohlc_hot_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · hot list ∩ not exploded

Cash book **-18.42%** ($8,158) · signal-only (no cash/fees) was -11.86%. Starts YES **9/30**. Fills 156 · skips 74 · realized $-1849.08.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at names that looked hot on the prior price/volume tape and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: names that looked hot on the prior price/volume tape.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior 5-session return is at least 0%.
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-have: prior relative volume is at most 2.2 (not a blow-off).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on names that looked hot on the prior price/volume tape that pass the must-haves.
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

- **Universe** `ohlc_hot` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $5,362.22.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ADUR` | 605 | — | $16.50 | +0.00 | $16.17 | -199.65 | -199.65 | +0.00 | -199.65 |
| 2026-08-17 | `ADUR` | 605 | $16.17 | $15.73 | -266.20 | — | +0.00 | -266.20 | -465.85 | — |
| 2026-08-17 | `OCC` | 173 | — | $18.24 | +0.00 | $17.12 | -193.76 | -193.76 | +0.00 | -193.76 |
| 2026-08-17 | `ALM` | 195 | — | $16.20 | +0.00 | $16.36 | +31.20 | +31.20 | +0.00 | +31.20 |
| 2026-08-17 | `NEWP` | 457 | — | $6.94 | +0.00 | $6.66 | -127.96 | -127.96 | +0.00 | -127.96 |
| 2026-08-18 | `OCC` | 173 | $17.12 | $16.20 | -159.16 | — | +0.00 | -159.16 | -352.92 | — |
| 2026-08-18 | `ALM` | 195 | $16.36 | $15.78 | -113.10 | — | +0.00 | -113.10 | -81.90 | — |
| 2026-08-18 | `NEWP` | 457 | $6.66 | $6.51 | -68.55 | — | +0.00 | -68.55 | -196.51 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `NIQ` | 121 | — | $18.31 | +0.00 | $18.15 | -19.36 | -19.36 | +0.00 | -19.36 |
| 2026-08-20 | `AUGO` | 26 | — | $83.58 | +0.00 | $86.69 | +80.86 | +80.86 | +0.00 | +80.86 |
| 2026-08-20 | `ZLAB` | 83 | — | $26.57 | +0.00 | $26.02 | -45.65 | -45.65 | +0.00 | -45.65 |
| 2026-08-20 | `PAYS` | 161 | — | $13.76 | +0.00 | $13.82 | +9.66 | +9.66 | +0.00 | +9.66 |
| 2026-08-21 | `NIQ` | 121 | $18.15 | $18.30 | +18.15 | — | +0.00 | +18.15 | -1.21 | — |
| 2026-08-21 | `AUGO` | 26 | $86.69 | $89.10 | +62.66 | — | +0.00 | +62.66 | +143.52 | — |
| 2026-08-21 | `ZLAB` | 83 | $26.02 | $26.25 | +19.09 | — | +0.00 | +19.09 | -26.56 | — |
| 2026-08-21 | `PAYS` | 161 | $13.82 | $13.91 | +14.49 | — | +0.00 | +14.49 | +24.15 | — |
| 2026-08-21 | `ORBS` | 2600 | — | $0.86 | +0.00 | $0.88 | +41.60 | +41.60 | +0.00 | +41.60 |
| 2026-08-21 | `EMBC` | 413 | — | $5.43 | +0.00 | $5.23 | -82.60 | -82.60 | +0.00 | -82.60 |
| 2026-08-21 | `TXG` | 34 | — | $64.39 | +0.00 | $65.12 | +24.82 | +24.82 | +0.00 | +24.82 |
| 2026-08-21 | `DXYZ` | 64 | — | $34.89 | +0.00 | $34.43 | -29.44 | -29.44 | +0.00 | -29.44 |
| 2026-08-24 | `ORBS` | 2600 | $0.88 | $0.89 | +26.00 | — | +0.00 | +26.00 | +67.60 | — |
| 2026-08-24 | `EMBC` | 413 | $5.23 | $5.20 | -14.46 | — | +0.00 | -14.46 | -97.05 | — |
| 2026-08-24 | `TXG` | 34 | $65.12 | $63.15 | -66.98 | — | +0.00 | -66.98 | -42.16 | — |
| 2026-08-24 | `DXYZ` | 64 | $34.43 | $33.10 | -85.12 | — | +0.00 | -85.12 | -114.56 | — |
| 2026-08-25 | `XHG` | 428 | — | $4.07 | +0.00 | $4.02 | -21.40 | -21.40 | +0.00 | -21.40 |
| 2026-08-25 | `AVAH` | 127 | — | $13.62 | +0.00 | $13.59 | -4.45 | -4.45 | +0.00 | -4.45 |
| 2026-08-25 | `ETON` | 27 | — | $64.55 | +0.00 | $63.05 | -40.50 | -40.50 | +0.00 | -40.50 |
| 2026-08-25 | `INO` | 1395 | — | $1.25 | +0.00 | $1.29 | +55.80 | +55.80 | +0.00 | +55.80 |
| 2026-08-25 | `ANRO` | 47 | — | $36.52 | +0.00 | $36.31 | -9.87 | -9.87 | +0.00 | -9.87 |
| 2026-08-26 | `XHG` | 428 | $4.02 | $3.81 | -89.88 | — | +0.00 | -89.88 | -111.28 | — |
| 2026-08-26 | `AVAH` | 127 | $13.59 | $13.65 | +7.62 | — | +0.00 | +7.62 | +3.18 | — |
| 2026-08-26 | `ETON` | 27 | $63.05 | $63.60 | +14.85 | — | +0.00 | +14.85 | -25.65 | — |
| 2026-08-26 | `INO` | 1395 | $1.29 | $1.28 | -13.95 | $1.30 | +27.90 | +13.95 | +41.85 | +69.75 |
| 2026-08-26 | `ANRO` | 47 | $36.31 | $35.80 | -23.97 | — | +0.00 | -23.97 | -33.84 | — |
| 2026-08-26 | `CRDL` | 3311 | — | $2.03 | +0.00 | $2.14 | +364.21 | +364.21 | +0.00 | +364.21 |
| 2026-08-27 | `INO` | 1395 | $1.30 | $1.29 | -13.95 | $1.26 | -41.85 | -55.80 | +55.80 | +13.95 |
| 2026-08-27 | `CRDL` | 3311 | $2.14 | $2.09 | -165.55 | — | +0.00 | -165.55 | +198.66 | — |
| 2026-08-27 | `HTFL` | 20 | — | $48.92 | +0.00 | $48.10 | -16.40 | -16.40 | +0.00 | -16.40 |
| 2026-08-27 | `NABL` | 253 | — | $3.87 | +0.00 | $3.95 | +20.24 | +20.24 | +0.00 | +20.24 |
| 2026-08-27 | `SRRK` | 16 | — | $60.00 | +0.00 | $59.26 | -11.84 | -11.84 | +0.00 | -11.84 |
| 2026-08-27 | `PAGP` | 35 | — | $28.00 | +0.00 | $28.03 | +1.05 | +1.05 | +0.00 | +1.05 |
| 2026-08-27 | `DASH` | 4 | — | $235.94 | +0.00 | $231.89 | -16.20 | -16.20 | +0.00 | -16.20 |
| 2026-08-27 | `PRGO` | 67 | — | $14.63 | +0.00 | $14.37 | -17.42 | -17.42 | +0.00 | -17.42 |
| 2026-08-27 | `AEO` | 56 | — | $17.27 | +0.00 | $16.69 | -32.48 | -32.48 | +0.00 | -32.48 |
| 2026-08-28 | `INO` | 1395 | $1.26 | $1.27 | +13.95 | — | +0.00 | +13.95 | +27.90 | — |
| 2026-08-28 | `HTFL` | 20 | $48.10 | $48.50 | +8.00 | — | +0.00 | +8.00 | -8.40 | — |
| 2026-08-28 | `NABL` | 253 | $3.95 | $4.25 | +75.90 | — | +0.00 | +75.90 | +96.14 | — |
| 2026-08-28 | `SRRK` | 16 | $59.26 | $58.75 | -8.16 | — | +0.00 | -8.16 | -20.00 | — |
| 2026-08-28 | `PAGP` | 35 | $28.03 | $28.08 | +1.75 | — | +0.00 | +1.75 | +2.80 | — |
| 2026-08-28 | `DASH` | 4 | $231.89 | $233.37 | +5.92 | — | +0.00 | +5.92 | -10.28 | — |
| 2026-08-28 | `PRGO` | 67 | $14.37 | $14.09 | -18.76 | — | +0.00 | -18.76 | -36.18 | — |
| 2026-08-28 | `AEO` | 56 | $16.69 | $17.06 | +20.72 | — | +0.00 | +20.72 | -11.76 | — |
| 2026-08-28 | `MRNA` | 20 | — | $137.19 | +0.00 | $137.99 | +16.00 | +16.00 | +0.00 | +16.00 |
| 2026-08-28 | `TH` | 151 | — | $19.00 | +0.00 | $18.55 | -67.95 | -67.95 | +0.00 | -67.95 |
| 2026-08-28 | `FSM` | 223 | — | $12.84 | +0.00 | $12.26 | -129.34 | -129.34 | +0.00 | -129.34 |
| 2026-08-31 | `MRNA` | 20 | $137.99 | $134.10 | -77.80 | — | +0.00 | -77.80 | -61.80 | — |
| 2026-08-31 | `TH` | 151 | $18.55 | $18.12 | -64.18 | — | +0.00 | -64.18 | -132.12 | — |
| 2026-08-31 | `FSM` | 223 | $12.26 | $12.26 | +0.00 | — | +0.00 | +0.00 | -129.34 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CABA` | 455 | — | $3.63 | +0.00 | $3.48 | -68.25 | -68.25 | +0.00 | -68.25 |
| 2026-09-03 | `ARCT` | 98 | — | $16.77 | +0.00 | $15.56 | -118.58 | -118.58 | +0.00 | -118.58 |
| 2026-09-03 | `EBS` | 252 | — | $6.56 | +0.00 | $6.23 | -83.16 | -83.16 | +0.00 | -83.16 |
| 2026-09-03 | `GALT` | 367 | — | $4.50 | +0.00 | $4.35 | -55.05 | -55.05 | +0.00 | -55.05 |
| 2026-09-03 | `CTVA` | 18 | — | $90.24 | +0.00 | $88.62 | -29.16 | -29.16 | +0.00 | -29.16 |
| 2026-09-04 | `CABA` | 455 | $3.48 | $3.46 | -9.10 | — | +0.00 | -9.10 | -77.35 | — |
| 2026-09-04 | `ARCT` | 98 | $15.56 | $15.61 | +4.90 | — | +0.00 | +4.90 | -113.68 | — |
| 2026-09-04 | `EBS` | 252 | $6.23 | $6.26 | +7.56 | — | +0.00 | +7.56 | -75.60 | — |
| 2026-09-04 | `GALT` | 367 | $4.35 | $4.33 | -7.34 | — | +0.00 | -7.34 | -62.39 | — |
| 2026-09-04 | `CTVA` | 18 | $88.62 | $87.64 | -17.64 | — | +0.00 | -17.64 | -46.80 | — |
| 2026-09-04 | `USDE` | 124 | — | $7.87 | +0.00 | $7.93 | +7.44 | +7.44 | +0.00 | +7.44 |
| 2026-09-04 | `GORO` | 248 | — | $3.95 | +0.00 | $4.15 | +49.60 | +49.60 | +0.00 | +49.60 |
| 2026-09-04 | `CRCL` | 10 | — | $97.98 | +0.00 | $102.05 | +40.70 | +40.70 | +0.00 | +40.70 |
| 2026-09-04 | `MSTR` | 7 | — | $137.35 | +0.00 | $142.80 | +38.15 | +38.15 | +0.00 | +38.15 |
| 2026-09-04 | `BLSH` | 28 | — | $34.69 | +0.00 | $36.00 | +36.68 | +36.68 | +0.00 | +36.68 |
| 2026-09-04 | `ZETA` | 30 | — | $32.65 | +0.00 | $31.35 | -39.00 | -39.00 | +0.00 | -39.00 |
| 2026-09-04 | `HAFN` | 109 | — | $8.94 | +0.00 | $9.22 | +30.52 | +30.52 | +0.00 | +30.52 |
| 2026-09-04 | `BE` | 4 | — | $236.82 | +0.00 | $252.87 | +64.20 | +64.20 | +0.00 | +64.20 |
| 2026-09-08 | `USDE` | 124 | $7.93 | $7.76 | -21.08 | — | +0.00 | -21.08 | -13.64 | — |
| 2026-09-08 | `GORO` | 248 | $4.15 | $4.13 | -4.96 | — | +0.00 | -4.96 | +44.64 | — |
| 2026-09-08 | `CRCL` | 10 | $102.05 | $100.65 | -14.00 | — | +0.00 | -14.00 | +26.70 | — |
| 2026-09-08 | `MSTR` | 7 | $142.80 | $137.62 | -36.26 | — | +0.00 | -36.26 | +1.89 | — |
| 2026-09-08 | `BLSH` | 28 | $36.00 | $35.90 | -2.80 | — | +0.00 | -2.80 | +33.88 | — |
| 2026-09-08 | `ZETA` | 30 | $31.35 | $31.08 | -8.10 | — | +0.00 | -8.10 | -47.10 | — |
| 2026-09-08 | `HAFN` | 109 | $9.22 | $8.81 | -44.69 | $8.96 | +16.35 | -28.34 | -14.17 | +2.18 |
| 2026-09-08 | `BE` | 4 | $252.87 | $267.76 | +59.56 | — | +0.00 | +59.56 | +123.76 | — |
| 2026-09-09 | `HAFN` | 109 | $8.96 | $9.00 | +4.36 | — | +0.00 | +4.36 | +6.54 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `AMTX` | 490 | — | $2.04 | +0.00 | $2.01 | -14.70 | -14.70 | +0.00 | -14.70 |
| 2026-09-11 | `CLOV` | 210 | — | $4.75 | +0.00 | $4.82 | +14.70 | +14.70 | +0.00 | +14.70 |
| 2026-09-11 | `BAK` | 471 | — | $2.12 | +0.00 | $2.08 | -18.84 | -18.84 | +0.00 | -18.84 |
| 2026-09-11 | `TJGC` | 93 | — | $10.65 | +0.00 | $11.19 | +50.22 | +50.22 | +0.00 | +50.22 |
| 2026-09-11 | `HAFN` | 107 | — | $9.32 | +0.00 | $9.38 | +6.42 | +6.42 | +0.00 | +6.42 |
| 2026-09-11 | `INSP` | 14 | — | $69.88 | +0.00 | $73.00 | +43.68 | +43.68 | +0.00 | +43.68 |
| 2026-09-11 | `FRO` | 20 | — | $48.05 | +0.00 | $49.21 | +23.20 | +23.20 | +0.00 | +23.20 |
| 2026-09-11 | `GME` | 47 | — | $21.04 | +0.00 | $21.15 | +5.17 | +5.17 | +0.00 | +5.17 |
| 2026-09-14 | `AMTX` | 490 | $2.01 | $2.01 | +0.00 | — | +0.00 | +0.00 | -14.70 | — |
| 2026-09-14 | `CLOV` | 210 | $4.82 | $4.82 | +0.00 | — | +0.00 | +0.00 | +14.70 | — |
| 2026-09-14 | `BAK` | 471 | $2.08 | $2.05 | -14.13 | — | +0.00 | -14.13 | -32.97 | — |
| 2026-09-14 | `TJGC` | 93 | $11.19 | $11.24 | +5.11 | — | +0.00 | +5.11 | +55.33 | — |
| 2026-09-14 | `HAFN` | 107 | $9.38 | $9.35 | -3.21 | — | +0.00 | -3.21 | +3.21 | — |
| 2026-09-14 | `INSP` | 14 | $73.00 | $72.14 | -12.04 | — | +0.00 | -12.04 | +31.64 | — |
| 2026-09-14 | `FRO` | 20 | $49.21 | $50.25 | +20.80 | $50.52 | +5.40 | +26.20 | +44.00 | +49.40 |
| 2026-09-14 | `GME` | 47 | $21.15 | $21.00 | -7.05 | $21.62 | +29.14 | +22.09 | -1.88 | +27.26 |
| 2026-09-15 | `FRO` | 20 | $50.52 | $51.21 | +13.80 | $51.59 | +7.60 | +21.40 | +63.20 | +70.80 |
| 2026-09-15 | `GME` | 47 | $21.62 | $21.51 | -5.17 | — | +0.00 | -5.17 | +22.09 | — |
| 2026-09-16 | `FRO` | 20 | $51.59 | $52.52 | +18.60 | — | +0.00 | +18.60 | +89.40 | — |
| 2026-09-16 | `META` | 1 | — | $679.91 | +0.00 | $673.31 | -6.60 | -6.60 | +0.00 | -6.60 |
| 2026-09-16 | `SM` | 25 | — | $39.99 | +0.00 | $38.16 | -45.75 | -45.75 | +0.00 | -45.75 |
| 2026-09-16 | `KR` | 16 | — | $61.93 | +0.00 | $61.14 | -12.64 | -12.64 | +0.00 | -12.64 |
| 2026-09-16 | `ATRC` | 18 | — | $55.66 | +0.00 | $57.14 | +26.64 | +26.64 | +0.00 | +26.64 |
| 2026-09-16 | `APA` | 21 | — | $46.44 | +0.00 | $44.79 | -34.65 | -34.65 | +0.00 | -34.65 |
| 2026-09-16 | `QCOM` | 5 | — | $189.17 | +0.00 | $184.84 | -21.65 | -21.65 | +0.00 | -21.65 |
| 2026-09-16 | `TK` | 71 | — | $14.26 | +0.00 | $14.39 | +9.23 | +9.23 | +0.00 | +9.23 |
| 2026-09-16 | `KGS` | 17 | — | $58.00 | +0.00 | $58.41 | +6.97 | +6.97 | +0.00 | +6.97 |
| 2026-09-17 | `META` | 1 | $673.31 | $682.44 | +9.13 | — | +0.00 | +9.13 | +2.53 | — |
| 2026-09-17 | `SM` | 25 | $38.16 | $37.57 | -14.75 | — | +0.00 | -14.75 | -60.50 | — |
| 2026-09-17 | `KR` | 16 | $61.14 | $61.02 | -1.92 | — | +0.00 | -1.92 | -14.56 | — |
| 2026-09-17 | `ATRC` | 18 | $57.14 | $57.96 | +14.76 | — | +0.00 | +14.76 | +41.40 | — |
| 2026-09-17 | `APA` | 21 | $44.79 | $44.63 | -3.36 | — | +0.00 | -3.36 | -38.01 | — |
| 2026-09-17 | `QCOM` | 5 | $184.84 | $190.35 | +27.55 | — | +0.00 | +27.55 | +5.90 | — |
| 2026-09-17 | `TK` | 71 | $14.39 | $14.41 | +1.42 | $14.67 | +18.46 | +19.88 | +10.65 | +29.11 |
| 2026-09-17 | `KGS` | 17 | $58.41 | $58.91 | +8.50 | $58.28 | -10.71 | -2.21 | +15.47 | +4.76 |
| 2026-09-17 | `PGEN` | 132 | — | $7.59 | +0.00 | $7.87 | +36.96 | +36.96 | +0.00 | +36.96 |
| 2026-09-17 | `SABR` | 418 | — | $2.40 | +0.00 | $2.32 | -33.44 | -33.44 | +0.00 | -33.44 |
| 2026-09-17 | `QTRX` | 341 | — | $2.94 | +0.00 | $3.12 | +61.38 | +61.38 | +0.00 | +61.38 |
| 2026-09-17 | `BNC` | 199 | — | $5.03 | +0.00 | $5.42 | +77.61 | +77.61 | +0.00 | +77.61 |
| 2026-09-17 | `SFL` | 74 | — | $13.55 | +0.00 | $13.75 | +14.80 | +14.80 | +0.00 | +14.80 |
| 2026-09-17 | `PUMP` | 96 | — | $10.31 | +0.00 | $10.46 | +14.40 | +14.40 | +0.00 | +14.40 |
| 2026-09-18 | `TK` | 71 | $14.67 | $14.60 | -4.97 | — | +0.00 | -4.97 | +24.14 | — |
| 2026-09-18 | `KGS` | 17 | $58.28 | $58.38 | +1.70 | — | +0.00 | +1.70 | +6.46 | — |
| 2026-09-18 | `PGEN` | 132 | $7.87 | $7.98 | +14.52 | — | +0.00 | +14.52 | +51.48 | — |
| 2026-09-18 | `SABR` | 418 | $2.32 | $2.29 | -12.54 | — | +0.00 | -12.54 | -45.98 | — |
| 2026-09-18 | `QTRX` | 341 | $3.12 | $3.12 | +0.00 | — | +0.00 | +0.00 | +61.38 | — |
| 2026-09-18 | `BNC` | 199 | $5.42 | $5.83 | +81.59 | $5.98 | +29.85 | +111.44 | +159.20 | +189.05 |
| 2026-09-18 | `SFL` | 74 | $13.75 | $13.74 | -0.74 | — | +0.00 | -0.74 | +14.06 | — |
| 2026-09-18 | `PUMP` | 96 | $10.46 | $10.48 | +1.92 | — | +0.00 | +1.92 | +16.32 | — |
| 2026-09-18 | `AMD` | 4 | — | $547.37 | +0.00 | $559.82 | +49.80 | +49.80 | +0.00 | +49.80 |
| 2026-09-18 | `SYM` | 53 | — | $44.70 | +0.00 | $41.89 | -148.93 | -148.93 | +0.00 | -148.93 |
| 2026-09-18 | `TH` | 113 | — | $20.91 | +0.00 | $21.19 | +31.64 | +31.64 | +0.00 | +31.64 |
| 2026-09-21 | `BNC` | 199 | $5.98 | $6.42 | +86.56 | — | +0.00 | +86.56 | +275.61 | — |
| 2026-09-21 | `AMD` | 4 | $559.82 | $583.88 | +96.24 | $615.52 | +126.56 | +222.80 | +146.04 | +272.60 |
| 2026-09-21 | `SYM` | 53 | $41.89 | $42.42 | +28.09 | — | +0.00 | +28.09 | -120.84 | — |
| 2026-09-21 | `TH` | 113 | $21.19 | $21.65 | +51.98 | — | +0.00 | +51.98 | +83.62 | — |
| 2026-09-21 | `BTDR` | 65 | — | $13.47 | +0.00 | $13.14 | -21.77 | -21.77 | +0.00 | -21.77 |
| 2026-09-21 | `MXL` | 10 | — | $83.53 | +0.00 | $85.98 | +24.50 | +24.50 | +0.00 | +24.50 |
| 2026-09-21 | `FORM` | 7 | — | $123.00 | +0.00 | $119.31 | -25.83 | -25.83 | +0.00 | -25.83 |
| 2026-09-21 | `COHR` | 2 | — | $326.48 | +0.00 | $321.52 | -9.92 | -9.92 | +0.00 | -9.92 |
| 2026-09-21 | `ASST` | 27 | — | $31.64 | +0.00 | $30.33 | -35.37 | -35.37 | +0.00 | -35.37 |
| 2026-09-21 | `SHMD` | 234 | — | $3.75 | +0.00 | $3.53 | -51.48 | -51.48 | +0.00 | -51.48 |
| 2026-09-21 | `TRMD` | 23 | — | $37.47 | +0.00 | $37.12 | -8.05 | -8.05 | +0.00 | -8.05 |
| 2026-09-22 | `AMD` | 4 | $615.52 | $606.57 | -35.80 | — | +0.00 | -35.80 | +236.80 | — |
| 2026-09-22 | `BTDR` | 65 | $13.14 | $13.14 | +0.00 | $13.14 | +0.00 | +0.00 | -21.77 | -21.77 |
| 2026-09-22 | `MXL` | 10 | $85.98 | $85.98 | +0.00 | $85.98 | +0.00 | +0.00 | +24.50 | +24.50 |
| 2026-09-22 | `FORM` | 7 | $119.31 | $119.31 | +0.00 | $119.31 | +0.00 | +0.00 | -25.83 | -25.83 |
| 2026-09-22 | `COHR` | 2 | $321.52 | $310.29 | -22.46 | — | +0.00 | -22.46 | -32.38 | — |
| 2026-09-22 | `ASST` | 27 | $30.33 | $29.30 | -27.81 | — | +0.00 | -27.81 | -63.18 | — |
| 2026-09-22 | `SHMD` | 234 | $3.53 | $3.53 | +0.00 | $3.53 | +0.00 | +0.00 | -51.48 | -51.48 |
| 2026-09-22 | `TRMD` | 23 | $37.12 | $37.12 | +0.00 | $37.12 | +0.00 | +0.00 | -8.05 | -8.05 |
| 2026-09-23 | `BTDR` | 65 | $13.14 | $12.84 | -19.50 | — | +0.00 | -19.50 | -41.27 | — |
| 2026-09-23 | `MXL` | 10 | $85.98 | $86.57 | +5.90 | — | +0.00 | +5.90 | +30.40 | — |
| 2026-09-23 | `FORM` | 7 | $119.31 | $125.39 | +42.56 | — | +0.00 | +42.56 | +16.73 | — |
| 2026-09-23 | `SHMD` | 234 | $3.53 | $3.61 | +18.72 | — | +0.00 | +18.72 | -32.76 | — |
| 2026-09-23 | `TRMD` | 23 | $37.12 | $34.83 | -52.67 | — | +0.00 | -52.67 | -60.72 | — |
| 2026-09-23 | `INDP` | 354 | — | $3.93 | +0.00 | $3.77 | -56.64 | -56.64 | +0.00 | -56.64 |
| 2026-09-23 | `NTSK` | 75 | — | $18.57 | +0.00 | $18.57 | -0.37 | -0.37 | +0.00 | -0.37 |
| 2026-09-23 | `ZS` | 6 | — | $213.00 | +0.00 | $214.45 | +8.70 | +8.70 | +0.00 | +8.70 |
| 2026-09-23 | `HIMS` | 45 | — | $30.40 | +0.00 | $28.39 | -90.45 | -90.45 | +0.00 | -90.45 |
| 2026-09-23 | `BLSH` | 34 | — | $40.00 | +0.00 | $40.11 | +3.74 | +3.74 | +0.00 | +3.74 |
| 2026-09-23 | `OPRT` | 169 | — | $8.23 | +0.00 | $8.48 | +42.25 | +42.25 | +0.00 | +42.25 |
| 2026-09-24 | `INDP` | 354 | $3.77 | $3.77 | +0.00 | — | +0.00 | +0.00 | -56.64 | — |
| 2026-09-24 | `NTSK` | 75 | $18.57 | $18.50 | -5.25 | $18.57 | +5.25 | +0.00 | -5.62 | -0.37 |
| 2026-09-24 | `ZS` | 6 | $214.45 | $213.47 | -5.85 | — | +0.00 | -5.85 | +2.85 | — |
| 2026-09-24 | `HIMS` | 45 | $28.39 | $28.00 | -17.55 | — | +0.00 | -17.55 | -108.00 | — |
| 2026-09-24 | `BLSH` | 34 | $40.11 | $39.27 | -28.56 | — | +0.00 | -28.56 | -24.82 | — |
| 2026-09-24 | `OPRT` | 169 | $8.48 | $8.47 | -1.69 | $8.30 | -28.73 | -30.42 | +40.56 | +11.83 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -199.65 | ADUR | — | $9.70 | $9,792.55 | ADUR×605 |
| 2026-08-17 | +2.25 | $9.70 | ADUR×605 | $9,526.35 | -266.20 | -290.52 | OCC, ALM, NEWP | ADUR | $21.29 | $9,216.87 | OCC×173, ALM×195, NEWP×457 |
| 2026-08-18 | -6.20 | $21.29 | OCC×173, ALM×195, NEWP×457 | $8,876.06 | -340.81 | +0.00 | — | OCC, ALM, NEWP | $8,864.87 | $8,864.87 | — |
| 2026-08-19 | -7.20 | $8,864.87 | — | $8,864.87 | -0.00 | +0.00 | — | — | $8,864.87 | $8,864.87 | — |
| 2026-08-20 | +1.12 | $8,864.87 | — | $8,864.87 | -0.00 | +25.51 | NIQ, AUGO, ZLAB, PAYS | — | $46.47 | $8,881.24 | NIQ×121, AUGO×26, ZLAB×83, PAYS×161 |
| 2026-08-21 | +3.25 | $46.47 | NIQ×121, AUGO×26, ZLAB×83, PAYS×161 | $8,995.63 | +114.39 | -45.62 | ORBS, EMBC, TXG, DXYZ | NIQ, AUGO, ZLAB, PAYS | $35.28 | $8,900.87 | ORBS×2600, EMBC×413, TXG×34, DXYZ×64 |
| 2026-08-24 | -5.17 | $35.28 | ORBS×2600, EMBC×413, TXG×34, DXYZ×64 | $8,760.32 | -140.55 | +0.00 | — | ORBS, EMBC, TXG, DXYZ | $8,719.19 | $8,719.19 | — |
| 2026-08-25 | +1.80 | $8,719.19 | — | $8,719.19 | -0.00 | -20.42 | XHG, AVAH, ETON, INO, ANRO | — | $13.72 | $8,668.68 | XHG×428, AVAH×127, ETON×27, INO×1395, ANRO×47 |
| 2026-08-26 | +2.02 | $13.72 | XHG×428, AVAH×127, ETON×27, INO×1395, ANRO×47 | $8,563.35 | -105.33 | +392.11 | CRDL | XHG, AVAH, ETON, ANRO | $1.45 | $8,900.49 | INO×1395, CRDL×3311 |
| 2026-08-27 | — | $1.45 | INO×1395, CRDL×3311 | $8,720.99 | -179.50 | -114.90 | HTFL, NABL, SRRK, PAGP, DASH, PRGO, AEO | CRDL | $73.73 | $8,546.98 | INO×1395, HTFL×20, NABL×253, SRRK×16, PAGP×35, DASH×4, PRGO×67, AEO×56 |
| 2026-08-28 | +0.75 | $73.73 | INO×1395, HTFL×20, NABL×253, SRRK×16, PAGP×35, DASH×4, PRGO×67, AEO×56 | $8,646.30 | +99.32 | -181.29 | MRNA, TH, FSM | INO, HTFL, NABL, SRRK, PAGP, DASH, PRGO, AEO | $128.59 | $8,423.42 | MRNA×20, TH×151, FSM×223 |
| 2026-08-31 | -5.85 | $128.59 | MRNA×20, TH×151, FSM×223 | $8,281.45 | -141.97 | +0.00 | — | MRNA, TH, FSM | $8,273.94 | $8,273.94 | — |
| 2026-09-01 | -6.30 | $8,273.94 | — | $8,273.94 | +0.00 | +0.00 | — | — | $8,273.94 | $8,273.94 | — |
| 2026-09-02 | -3.83 | $8,273.94 | — | $8,273.94 | +0.00 | +0.00 | — | — | $8,273.94 | $8,273.94 | — |
| 2026-09-03 | -0.90 | $8,273.94 | — | $8,273.94 | +0.00 | -354.20 | CABA, ARCT, EBS, GALT, CTVA | — | $31.71 | $7,901.56 | CABA×455, ARCT×98, EBS×252, GALT×367, CTVA×18 |
| 2026-09-04 | +2.25 | $31.71 | CABA×455, ARCT×98, EBS×252, GALT×367, CTVA×18 | $7,879.94 | -21.62 | +228.29 | USDE, GORO, CRCL, MSTR, BLSH, ZETA, HAFN, BE | CABA, ARCT, EBS, GALT, CTVA | $74.13 | $8,071.71 | USDE×124, GORO×248, CRCL×10, MSTR×7, BLSH×28, ZETA×30, HAFN×109, BE×4 |
| 2026-09-08 | -11.47 | $74.13 | USDE×124, GORO×248, CRCL×10, MSTR×7, BLSH×28, ZETA×30, HAFN×109, BE×4 | $7,999.38 | -72.33 | +16.35 | — | USDE, GORO, CRCL, MSTR, BLSH, ZETA, BE | $7,023.16 | $7,999.80 | HAFN×109 |
| 2026-09-09 | -13.95 | $7,023.16 | HAFN×109 | $8,004.16 | +4.36 | +0.00 | — | HAFN | $8,001.82 | $8,001.82 | — |
| 2026-09-10 | -13.28 | $8,001.82 | — | $8,001.82 | -0.00 | +0.00 | — | — | $8,001.82 | $8,001.82 | — |
| 2026-09-11 | +0.50 | $8,001.82 | — | $8,001.82 | -0.00 | +109.85 | AMTX, CLOV, BAK, TJGC, HAFN, INSP, FRO, GME | — | $64.41 | $8,085.77 | AMTX×490, CLOV×210, BAK×471, TJGC×93, HAFN×107, INSP×14, FRO×20, GME×47 |
| 2026-09-14 | -11.00 | $64.41 | AMTX×490, CLOV×210, BAK×471, TJGC×93, HAFN×107, INSP×14, FRO×20, GME×47 | $8,075.25 | -10.52 | +34.54 | — | AMTX, CLOV, BAK, TJGC, HAFN, INSP | $6,061.24 | $8,087.78 | FRO×20, GME×47 |
| 2026-09-15 | -3.84 | $6,061.24 | FRO×20, GME×47 | $8,096.41 | +8.63 | +7.60 | — | GME | $7,070.06 | $8,101.86 | FRO×20 |
| 2026-09-16 | +5.30 | $7,070.06 | FRO×20 | $8,120.46 | +18.60 | -78.45 | META, SM, KR, ATRC, APA, QCOM, TK, KGS | FRO | $509.98 | $8,023.50 | META×1, SM×25, KR×16, ATRC×18, APA×21, QCOM×5, TK×71, KGS×17 |
| 2026-09-17 | +7.38 | $509.98 | META×1, SM×25, KR×16, ATRC×18, APA×21, QCOM×5, TK×71, KGS×17 | $8,064.83 | +41.33 | +179.46 | PGEN, SABR, QTRX, BNC, SFL, PUMP | META, SM, KR, ATRC, APA, QCOM | $7.62 | $8,212.71 | TK×71, KGS×17, PGEN×132, SABR×418, QTRX×341, BNC×199, SFL×74, PUMP×96 |
| 2026-09-18 | +4.86 | $7.62 | TK×71, KGS×17, PGEN×132, SABR×418, QTRX×341, BNC×199, SFL×74, PUMP×96 | $8,294.19 | +81.48 | -37.64 | AMD, SYM, TH | TK, KGS, PGEN, SABR, QTRX, SFL, PUMP | $184.96 | $8,228.90 | BNC×199, AMD×4, SYM×53, TH×113 |
| 2026-09-21 | +12.87 | $184.96 | BNC×199, AMD×4, SYM×53, TH×113 | $8,491.77 | +262.87 | -1.36 | BTDR, MXL, FORM, COHR, ASST, SHMD, TRMD | BNC, SYM, TH | $314.99 | $8,467.87 | AMD×4, BTDR×65, MXL×10, FORM×7, COHR×2, ASST×27, SHMD×234, TRMD×23 |
| 2026-09-22 | -0.50 | $314.99 | AMD×4, BTDR×65, MXL×10, FORM×7, COHR×2, ASST×27, SHMD×234, TRMD×23 | $8,381.80 | -86.07 | +0.00 | — | AMD, COHR, ASST | $4,146.81 | $8,375.66 | BTDR×65, MXL×10, FORM×7, SHMD×234, TRMD×23 |
| 2026-09-23 | +2.29 | $4,146.81 | BTDR×65, MXL×10, FORM×7, SHMD×234, TRMD×23 | $8,370.67 | -4.99 | -92.77 | INDP, NTSK, ZS, HIMS, BLSH, OPRT | BTDR, MXL, FORM, SHMD, TRMD | $162.53 | $8,250.97 | INDP×354, NTSK×75, ZS×6, HIMS×45, BLSH×34, OPRT×169 |
| 2026-09-24 | -7.66 | $162.53 | INDP×354, NTSK×75, ZS×6, HIMS×45, BLSH×34, OPRT×169 | $8,192.07 | -58.90 | -23.48 | — | INDP, ZS, HIMS, BLSH | $5,362.22 | $8,157.67 | NTSK×75, OPRT×169 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 605 | $16.50 | $7.80 | — | $9.70 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $10000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.70 | ▼ close $9,792.55 vs 09:30 $10,000.00 (session -199.65) | 16:00 close · cash $9.70 · equity $9,792.55 vs 09:30 $10,000.00 (-207.45; session marks -199.65) · 1 name(s) marked open→close (per-name table). ADUR×605 09:30 $16.50 → close $16.17 -199.65 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.70 | ▼ 09:30 equity $9,526.35 vs yday $9,792.55 (-266.20) | 09:30 open · cash $9.70 (unchanged overnight, no fees) · equity $9,526.35 vs prior close $9,792.55 (-266.20) · 1 name(s) re-marked at the open (per-name table). ADUR×605 yday $16.17 → 09:30 $15.73 -266.20 | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 605 | $15.73 | $7.98 | $-481.64 | $9,518.36 | ▼ -481.64 after sell → book $9,518.36; vs 09:30 mark -7.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 173 | $18.24 | $2.51 | — | $6,360.34 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $3172.79 | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 195 | $16.20 | $2.58 | — | $3,198.76 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $3172.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NEWP` | 457 | $6.94 | $5.90 | — | $21.29 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.1; leftover $3172.79 | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.29 | ▼ close $9,216.87 vs 09:30 $9,526.35 (session -290.52) | 16:00 close · cash $21.29 · equity $9,216.87 vs 09:30 $9,526.35 (-309.48; session marks -290.52) · 3 name(s) marked open→close (per-name table). OCC×173 09:30 $18.24 → close $17.12 -193.76; ALM×195 09:30 $16.20 → close $16.36 +31.20; NEWP×457 09:30 $6.94 → close $6.66 -127.96 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.29 | ▼ 09:30 equity $8,876.06 vs yday $9,216.87 (-340.81) | 09:30 open · cash $21.29 (unchanged overnight, no fees) · equity $8,876.06 vs prior close $9,216.87 (-340.81) · 3 name(s) re-marked at the open (per-name table). OCC×173 yday $17.12 → 09:30 $16.20 -159.16; ALM×195 yday $16.36 → 09:30 $15.78 -113.10; NEWP×457 yday $6.66 → 09:30 $6.51 -68.55 | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 173 | $16.20 | $2.56 | $-357.99 | $2,821.32 | ▼ -357.99 after sell → book $8,873.49; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 195 | $15.78 | $2.63 | $-87.11 | $5,895.79 | ▼ -87.11 after sell → book $8,870.86; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NEWP` | 457 | $6.51 | $6.00 | $-208.40 | $8,864.87 | ▼ -208.40 after sell → book $8,864.87; vs 09:30 mark -5.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,864.87 | ▲ close $8,864.87 vs 09:30 $8,876.06 (session +0.00) | 16:00 close · cash $8,864.87 · no lots left · equity $8,864.87. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,864.87 | ▲ 09:30 equity $8,864.87 vs yday $8,864.87 (-0.00) | 09:30 open · cash $8,864.87 · no holdings · equity $8,864.87 vs prior close $8,864.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,864.87 | ▲ close $8,864.87 vs 09:30 $8,864.87 (session +0.00) | 16:00 close · cash $8,864.87 · no lots left · equity $8,864.87. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,864.87 | ▲ 09:30 equity $8,864.87 vs yday $8,864.87 (-0.00) | 09:30 open · cash $8,864.87 · no holdings · equity $8,864.87 vs prior close $8,864.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `NIQ` | 121 | $18.31 | $2.35 | — | $6,647.00 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.9; leftover $2216.22 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUGO` | 26 | $83.58 | $2.07 | — | $4,471.86 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.6; leftover $2216.22 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 83 | $26.57 | $2.24 | — | $2,264.31 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.8; leftover $2216.22 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `PAYS` | 161 | $13.76 | $2.47 | — | $46.47 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.8; leftover $2216.22 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.47 | ▲ close $8,881.24 vs 09:30 $8,864.87 (session +25.51) | 16:00 close · cash $46.47 · equity $8,881.24 vs 09:30 $8,864.87 (+16.37; session marks +25.51) · 4 name(s) marked open→close (per-name table). NIQ×121 09:30 $18.31 → close $18.15 -19.36; AUGO×26 09:30 $83.58 → close $86.69 +80.86; ZLAB×83 09:30 $26.57 → close $26.02 -45.65; PAYS×161 09:30 $13.76 → close $13.82 +9.66 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.47 | ▲ 09:30 equity $8,995.63 vs yday $8,881.24 (+114.39) | 09:30 open · cash $46.47 (unchanged overnight, no fees) · equity $8,995.63 vs prior close $8,881.24 (+114.39) · 4 name(s) re-marked at the open (per-name table). NIQ×121 yday $18.15 → 09:30 $18.30 +18.15; AUGO×26 yday $86.69 → 09:30 $89.10 +62.66; ZLAB×83 yday $26.02 → 09:30 $26.25 +19.09; PAYS×161 yday $13.82 → 09:30 $13.91 +14.49 | — |
| 2026-08-21 09:30 ET | **SELL** | `NIQ` | 121 | $18.30 | $2.39 | $-5.95 | $2,258.38 | ▼ -5.95 after sell → book $8,993.24; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUGO` | 26 | $89.10 | $2.10 | $+139.36 | $4,572.89 | ▲ +139.36 after sell → book $8,991.15; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 83 | $26.25 | $2.27 | $-31.07 | $6,749.37 | ▼ -31.07 after sell → book $8,988.88; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `PAYS` | 161 | $13.91 | $2.52 | $+19.16 | $8,986.36 | ▲ +19.16 after sell → book $8,986.36; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 2600 | $0.86 | $30.26 | — | $6,709.70 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $2246.59 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 413 | $5.43 | $5.33 | — | $4,461.78 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $2246.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 34 | $64.39 | $2.09 | — | $2,270.43 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $2246.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 64 | $34.89 | $2.18 | — | $35.28 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6; leftover $2246.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.28 | ▼ close $8,900.87 vs 09:30 $8,995.63 (session -45.62) | 16:00 close · cash $35.28 · equity $8,900.87 vs 09:30 $8,995.63 (-94.76; session marks -45.62) · 4 name(s) marked open→close (per-name table). ORBS×2600 09:30 $0.86 → close $0.88 +41.60; EMBC×413 09:30 $5.43 → close $5.23 -82.60; TXG×34 09:30 $64.39 → close $65.12 +24.82; DXYZ×64 09:30 $34.89 → close $34.43 -29.44 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.28 | ▼ 09:30 equity $8,760.32 vs yday $8,900.87 (-140.55) | 09:30 open · cash $35.28 (unchanged overnight, no fees) · equity $8,760.32 vs prior close $8,900.87 (-140.55) · 4 name(s) re-marked at the open (per-name table). ORBS×2600 yday $0.88 → 09:30 $0.89 +26.00; EMBC×413 yday $5.23 → 09:30 $5.20 -14.46; TXG×34 yday $65.12 → 09:30 $63.15 -66.98; DXYZ×64 yday $34.43 → 09:30 $33.10 -85.12 | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 2600 | $0.89 | $31.39 | $+5.95 | $2,317.89 | ▲ +5.95 after sell → book $8,728.93; vs 09:30 mark -31.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 413 | $5.20 | $5.41 | $-107.80 | $4,458.02 | ▼ -107.80 after sell → book $8,723.52; vs 09:30 mark -5.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 34 | $63.15 | $2.12 | $-46.37 | $6,603.00 | ▼ -46.37 after sell → book $8,721.40; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 64 | $33.10 | $2.21 | $-118.95 | $8,719.19 | ▼ -118.95 after sell → book $8,719.19; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,719.19 | ▲ close $8,719.19 vs 09:30 $8,760.32 (session +0.00) | 16:00 close · cash $8,719.19 · no lots left · equity $8,719.19. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,719.19 | ▲ 09:30 equity $8,719.19 vs yday $8,719.19 (-0.00) | 09:30 open · cash $8,719.19 · no holdings · equity $8,719.19 vs prior close $8,719.19 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 428 | $4.07 | $5.52 | — | $6,971.71 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.9; leftover $1743.84 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 127 | $13.62 | $2.37 | — | $5,238.96 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1743.84 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 27 | $64.55 | $2.07 | — | $3,494.04 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.4; leftover $1743.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `INO` | 1395 | $1.25 | $18.00 | — | $1,732.29 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.7; leftover $1743.84 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ANRO` | 47 | $36.52 | $2.13 | — | $13.72 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.9; leftover $1743.84 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.72 | ▼ close $8,668.68 vs 09:30 $8,719.19 (session -20.42) | 16:00 close · cash $13.72 · equity $8,668.68 vs 09:30 $8,719.19 (-50.51; session marks -20.42) · 5 name(s) marked open→close (per-name table). XHG×428 09:30 $4.07 → close $4.02 -21.40; AVAH×127 09:30 $13.62 → close $13.59 -4.45; ETON×27 09:30 $64.55 → close $63.05 -40.50; INO×1395 09:30 $1.25 → close $1.29 +55.80; ANRO×47 09:30 $36.52 → close $36.31 -9.87 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.72 | ▼ 09:30 equity $8,563.35 vs yday $8,668.68 (-105.33) | 09:30 open · cash $13.72 (unchanged overnight, no fees) · equity $8,563.35 vs prior close $8,668.68 (-105.33) · 5 name(s) re-marked at the open (per-name table). XHG×428 yday $4.02 → 09:30 $3.81 -89.88; AVAH×127 yday $13.59 → 09:30 $13.65 +7.62; ETON×27 yday $63.05 → 09:30 $63.60 +14.85; INO×1395 yday $1.29 → 09:30 $1.28 -13.95; ANRO×47 yday $36.31 → 09:30 $35.80 -23.97 | — |
| 2026-08-26 09:30 ET | **SELL** | `XHG` | 428 | $3.81 | $5.61 | $-122.41 | $1,638.80 | ▼ -122.41 after sell → book $8,557.75; vs 09:30 mark -5.60 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `AVAH` | 127 | $13.65 | $2.41 | $-1.60 | $3,369.94 | ▼ -1.60 after sell → book $8,555.34; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 27 | $63.60 | $2.09 | $-29.82 | $5,085.05 | ▼ -29.82 after sell → book $8,553.25; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ANRO` | 47 | $35.80 | $2.15 | $-38.13 | $6,765.49 | ▼ -38.13 after sell → book $8,551.09; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CRDL` | 3311 | $2.03 | $42.71 | — | $1.45 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+5.5; leftover $6765.49 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.45 | ▲ close $8,900.49 vs 09:30 $8,563.35 (session +392.11) | 16:00 close · cash $1.45 · equity $8,900.49 vs 09:30 $8,563.35 (+337.14; session marks +392.11) · 2 name(s) marked open→close (per-name table). INO×1395 09:30 $1.28 → close $1.30 +27.90; CRDL×3311 09:30 $2.03 → close $2.14 +364.21 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.45 | ▼ 09:30 equity $8,720.99 vs yday $8,900.49 (-179.50) | 09:30 open · cash $1.45 (unchanged overnight, no fees) · equity $8,720.99 vs prior close $8,900.49 (-179.50) · 2 name(s) re-marked at the open (per-name table). INO×1395 yday $1.30 → 09:30 $1.29 -13.95; CRDL×3311 yday $2.14 → 09:30 $2.09 -165.55 | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 3311 | $2.09 | $43.32 | $+112.63 | $6,878.12 | ▲ +112.63 after sell → book $8,677.67; vs 09:30 mark -43.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `HTFL` | 20 | $48.92 | $2.05 | — | $5,897.67 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.5; leftover $982.59 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NABL` | 253 | $3.87 | $3.26 | — | $4,915.30 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.8; leftover $982.59 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 16 | $60.00 | $2.04 | — | $3,953.26 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+6.2; leftover $982.59 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PAGP` | 35 | $28.00 | $2.10 | — | $2,971.17 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.8; leftover $982.59 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DASH` | 4 | $235.94 | $2.00 | — | $2,025.40 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.6; leftover $982.59 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PRGO` | 67 | $14.63 | $2.19 | — | $1,043.00 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+5.7; leftover $982.59 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `AEO` | 56 | $17.27 | $2.16 | — | $73.73 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+5.5; leftover $982.59 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.73 | ▼ close $8,546.98 vs 09:30 $8,720.99 (session -114.90) | 16:00 close · cash $73.73 · equity $8,546.98 vs 09:30 $8,720.99 (-174.01; session marks -114.90) · 8 name(s) marked open→close (per-name table). INO×1395 09:30 $1.29 → close $1.26 -41.85; HTFL×20 09:30 $48.92 → close $48.10 -16.40; NABL×253 09:30 $3.87 → close $3.95 +20.24; SRRK×16 09:30 $60.00 → close $59.26 -11.84; PAGP×35 09:30 $28.00 → close $28.03 +1.05; DASH×4 09:30 $235.94 → close $231.89 -16.20; PRGO×67 09:30 $14.63 → close $14.37 -17.42; AEO×56 09:30 $17.27 → close $16.69 -32.48 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.73 | ▲ 09:30 equity $8,646.30 vs yday $8,546.98 (+99.32) | 09:30 open · cash $73.73 (unchanged overnight, no fees) · equity $8,646.30 vs prior close $8,546.98 (+99.32) · 8 name(s) re-marked at the open (per-name table). INO×1395 yday $1.26 → 09:30 $1.27 +13.95; HTFL×20 yday $48.10 → 09:30 $48.50 +8.00; NABL×253 yday $3.95 → 09:30 $4.25 +75.90; SRRK×16 yday $59.26 → 09:30 $58.75 -8.16; PAGP×35 yday $28.03 → 09:30 $28.08 +1.75; DASH×4 yday $231.89 → 09:30 $233.37 +5.92; PRGO×67 yday $14.37 → 09:30 $14.09 -18.76; AEO×56 yday $16.69 → 09:30 $17.06 +20.72 | — |
| 2026-08-28 09:30 ET | **SELL** | `INO` | 1395 | $1.27 | $18.24 | $-8.34 | $1,827.13 | ▼ -8.34 after sell → book $8,628.05; vs 09:30 mark -18.25 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HTFL` | 20 | $48.50 | $2.07 | $-12.52 | $2,795.06 | ▼ -12.52 after sell → book $8,625.98; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NABL` | 253 | $4.25 | $3.32 | $+89.56 | $3,867.00 | ▲ +89.56 after sell → book $8,622.67; vs 09:30 mark -3.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 16 | $58.75 | $2.06 | $-24.10 | $4,804.94 | ▼ -24.10 after sell → book $8,620.61; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PAGP` | 35 | $28.08 | $2.12 | $-1.41 | $5,785.63 | ▼ -1.41 after sell → book $8,618.50; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DASH` | 4 | $233.37 | $2.02 | $-14.30 | $6,717.08 | ▼ -14.30 after sell → book $8,616.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PRGO` | 67 | $14.09 | $2.21 | $-40.58 | $7,658.90 | ▼ -40.58 after sell → book $8,614.26; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEO` | 56 | $17.06 | $2.18 | $-16.10 | $8,612.08 | ▼ -16.10 after sell → book $8,612.08; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 20 | $137.19 | $2.05 | — | $5,866.23 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.1; leftover $2870.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 151 | $19.00 | $2.44 | — | $2,994.79 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.5; leftover $2870.69 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FSM` | 223 | $12.84 | $2.88 | — | $128.59 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.6; leftover $2870.69 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.59 | ▼ close $8,423.42 vs 09:30 $8,646.30 (session -181.29) | 16:00 close · cash $128.59 · equity $8,423.42 vs 09:30 $8,646.30 (-222.88; session marks -181.29) · 3 name(s) marked open→close (per-name table). MRNA×20 09:30 $137.19 → close $137.99 +16.00; TH×151 09:30 $19.00 → close $18.55 -67.95; FSM×223 09:30 $12.84 → close $12.26 -129.34 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.59 | ▼ 09:30 equity $8,281.45 vs yday $8,423.42 (-141.97) | 09:30 open · cash $128.59 (unchanged overnight, no fees) · equity $8,281.45 vs prior close $8,423.42 (-141.97) · 3 name(s) re-marked at the open (per-name table). MRNA×20 yday $137.99 → 09:30 $134.10 -77.80; TH×151 yday $18.55 → 09:30 $18.12 -64.18; FSM×223 yday $12.26 → 09:30 $12.26 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 20 | $134.10 | $2.08 | $-65.93 | $2,808.51 | ▼ -65.93 after sell → book $8,279.37; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 151 | $18.12 | $2.49 | $-137.06 | $5,542.90 | ▼ -137.06 after sell → book $8,276.88; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FSM` | 223 | $12.26 | $2.94 | $-135.15 | $8,273.94 | ▼ -135.15 after sell → book $8,273.94; vs 09:30 mark -2.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,273.94 | ▲ close $8,273.94 vs 09:30 $8,281.45 (session +0.00) | 16:00 close · cash $8,273.94 · no lots left · equity $8,273.94. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,273.94 | ▲ 09:30 equity $8,273.94 vs yday $8,273.94 (+0.00) | 09:30 open · cash $8,273.94 · no holdings · equity $8,273.94 vs prior close $8,273.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,273.94 | ▲ close $8,273.94 vs 09:30 $8,273.94 (session +0.00) | 16:00 close · cash $8,273.94 · no lots left · equity $8,273.94. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,273.94 | ▲ 09:30 equity $8,273.94 vs yday $8,273.94 (+0.00) | 09:30 open · cash $8,273.94 · no holdings · equity $8,273.94 vs prior close $8,273.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,273.94 | ▲ close $8,273.94 vs 09:30 $8,273.94 (session +0.00) | 16:00 close · cash $8,273.94 · no lots left · equity $8,273.94. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,273.94 | ▲ 09:30 equity $8,273.94 vs yday $8,273.94 (+0.00) | 09:30 open · cash $8,273.94 · no holdings · equity $8,273.94 vs prior close $8,273.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 455 | $3.63 | $5.87 | — | $6,616.42 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1654.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 98 | $16.77 | $2.28 | — | $4,970.68 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1654.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EBS` | 252 | $6.56 | $3.25 | — | $3,314.31 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ⚪; ret5=+8.2; leftover $1654.79 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GALT` | 367 | $4.50 | $4.73 | — | $1,658.07 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.0; leftover $1654.79 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTVA` | 18 | $90.24 | $2.04 | — | $31.71 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6; leftover $1654.79 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.71 | ▼ close $7,901.56 vs 09:30 $8,273.94 (session -354.20) | 16:00 close · cash $31.71 · equity $7,901.56 vs 09:30 $8,273.94 (-372.38; session marks -354.20) · 5 name(s) marked open→close (per-name table). CABA×455 09:30 $3.63 → close $3.48 -68.25; ARCT×98 09:30 $16.77 → close $15.56 -118.58; EBS×252 09:30 $6.56 → close $6.23 -83.16; GALT×367 09:30 $4.50 → close $4.35 -55.05; CTVA×18 09:30 $90.24 → close $88.62 -29.16 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.71 | ▼ 09:30 equity $7,879.94 vs yday $7,901.56 (-21.62) | 09:30 open · cash $31.71 (unchanged overnight, no fees) · equity $7,879.94 vs prior close $7,901.56 (-21.62) · 5 name(s) re-marked at the open (per-name table). CABA×455 yday $3.48 → 09:30 $3.46 -9.10; ARCT×98 yday $15.56 → 09:30 $15.61 +4.90; EBS×252 yday $6.23 → 09:30 $6.26 +7.56; GALT×367 yday $4.35 → 09:30 $4.33 -7.34; CTVA×18 yday $88.62 → 09:30 $87.64 -17.64 | — |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 455 | $3.46 | $5.96 | $-89.18 | $1,600.05 | ▼ -89.18 after sell → book $7,873.98; vs 09:30 mark -5.96 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 98 | $15.61 | $2.31 | $-118.28 | $3,127.52 | ▼ -118.28 after sell → book $7,871.67; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EBS` | 252 | $6.26 | $3.31 | $-82.16 | $4,701.73 | ▼ -82.16 after sell → book $7,868.36; vs 09:30 mark -3.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GALT` | 367 | $4.33 | $4.81 | $-71.93 | $6,286.04 | ▼ -71.93 after sell → book $7,863.56; vs 09:30 mark -4.80 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTVA` | 18 | $87.64 | $2.07 | $-50.91 | $7,861.49 | ▼ -50.91 after sell → book $7,861.49; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 124 | $7.87 | $2.36 | — | $6,883.25 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.7; leftover $982.69 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 248 | $3.95 | $3.20 | — | $5,900.45 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+6.9; leftover $982.69 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRCL` | 10 | $97.98 | $2.02 | — | $4,918.63 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.5; leftover $982.69 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 7 | $137.35 | $2.01 | — | $3,955.17 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+5.4; leftover $982.69 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BLSH` | 28 | $34.69 | $2.07 | — | $2,981.77 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.9; leftover $982.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ZETA` | 30 | $32.65 | $2.08 | — | $2,000.19 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.1; leftover $982.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 109 | $8.94 | $2.32 | — | $1,023.42 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.7; leftover $982.69 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 4 | $236.82 | $2.00 | — | $74.13 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.1; leftover $982.69 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.13 | ▲ close $8,071.71 vs 09:30 $7,879.94 (session +228.29) | 16:00 close · cash $74.13 · equity $8,071.71 vs 09:30 $7,879.94 (+191.77; session marks +228.29) · 8 name(s) marked open→close (per-name table). USDE×124 09:30 $7.87 → close $7.93 +7.44; GORO×248 09:30 $3.95 → close $4.15 +49.60; CRCL×10 09:30 $97.98 → close $102.05 +40.70; MSTR×7 09:30 $137.35 → close $142.80 +38.15; BLSH×28 09:30 $34.69 → close $36.00 +36.68; ZETA×30 09:30 $32.65 → close $31.35 -39.00; HAFN×109 09:30 $8.94 → close $9.22 +30.52; BE×4 09:30 $236.82 → close $252.87 +64.20 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.13 | ▼ 09:30 equity $7,999.38 vs yday $8,071.71 (-72.33) | 09:30 open · cash $74.13 (unchanged overnight, no fees) · equity $7,999.38 vs prior close $8,071.71 (-72.33) · 8 name(s) re-marked at the open (per-name table). USDE×124 yday $7.93 → 09:30 $7.76 -21.08; GORO×248 yday $4.15 → 09:30 $4.13 -4.96; CRCL×10 yday $102.05 → 09:30 $100.65 -14.00; MSTR×7 yday $142.80 → 09:30 $137.62 -36.26; BLSH×28 yday $36.00 → 09:30 $35.90 -2.80; ZETA×30 yday $31.35 → 09:30 $31.08 -8.10; HAFN×109 yday $9.22 → 09:30 $8.81 -44.69; BE×4 yday $252.87 → 09:30 $267.76 +59.56 | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 124 | $7.76 | $2.39 | $-18.39 | $1,033.98 | ▼ -18.39 after sell → book $7,996.99; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `GORO` | 248 | $4.13 | $3.25 | $+38.19 | $2,054.97 | ▲ +38.19 after sell → book $7,993.74; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `CRCL` | 10 | $100.65 | $2.04 | $+22.64 | $3,059.43 | ▲ +22.64 after sell → book $7,991.70; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 7 | $137.62 | $2.03 | $-2.15 | $4,020.74 | ▼ -2.15 after sell → book $7,989.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `BLSH` | 28 | $35.90 | $2.09 | $+29.71 | $5,023.85 | ▲ +29.71 after sell → book $7,987.58; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZETA` | 30 | $31.08 | $2.10 | $-51.28 | $5,954.15 | ▼ -51.28 after sell → book $7,985.48; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 4 | $267.76 | $2.02 | $+119.74 | $7,023.16 | ▲ +119.74 after sell → book $7,983.45; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,023.16 | ▲ close $7,999.80 vs 09:30 $7,999.38 (session +16.35) | 16:00 close · cash $7,023.16 · equity $7,999.80 vs 09:30 $7,999.38 (+0.42; session marks +16.35) · 1 name(s) marked open→close (per-name table). HAFN×109 09:30 $8.81 → close $8.96 +16.35 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,023.16 | ▲ 09:30 equity $8,004.16 vs yday $7,999.80 (+4.36) | 09:30 open · cash $7,023.16 (unchanged overnight, no fees) · equity $8,004.16 vs prior close $7,999.80 (+4.36) · 1 name(s) re-marked at the open (per-name table). HAFN×109 yday $8.96 → 09:30 $9.00 +4.36 | — |
| 2026-09-09 09:30 ET | **SELL** | `HAFN` | 109 | $9.00 | $2.35 | $+1.88 | $8,001.82 | ▲ +1.88 after sell → book $8,001.82; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,001.82 | ▲ close $8,001.82 vs 09:30 $8,004.16 (session +0.00) | 16:00 close · cash $8,001.82 · no lots left · equity $8,001.82. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,001.82 | ▲ 09:30 equity $8,001.82 vs yday $8,001.82 (-0.00) | 09:30 open · cash $8,001.82 · no holdings · equity $8,001.82 vs prior close $8,001.82 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,001.82 | ▲ close $8,001.82 vs 09:30 $8,001.82 (session +0.00) | 16:00 close · cash $8,001.82 · no lots left · equity $8,001.82. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,001.82 | ▲ 09:30 equity $8,001.82 vs yday $8,001.82 (-0.00) | 09:30 open · cash $8,001.82 · no holdings · equity $8,001.82 vs prior close $8,001.82 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 490 | $2.04 | $6.32 | — | $6,995.90 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1000.23 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 210 | $4.75 | $2.71 | — | $5,995.69 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1000.23 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 471 | $2.12 | $6.08 | — | $4,991.09 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1000.23 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `TJGC` | 93 | $10.65 | $2.27 | — | $3,998.37 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+6.3; leftover $1000.23 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟡 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `HAFN` | 107 | $9.32 | $2.31 | — | $2,998.82 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+5.4; leftover $1000.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 14 | $69.88 | $2.03 | — | $2,018.47 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.0; leftover $1000.23 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `FRO` | 20 | $48.05 | $2.05 | — | $1,055.42 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+6.6; leftover $1000.23 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `GME` | 47 | $21.04 | $2.13 | — | $64.41 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.5; leftover $1000.23 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.41 | ▲ close $8,085.77 vs 09:30 $8,001.82 (session +109.85) | 16:00 close · cash $64.41 · equity $8,085.77 vs 09:30 $8,001.82 (+83.95; session marks +109.85) · 8 name(s) marked open→close (per-name table). AMTX×490 09:30 $2.04 → close $2.01 -14.70; CLOV×210 09:30 $4.75 → close $4.82 +14.70; BAK×471 09:30 $2.12 → close $2.08 -18.84; TJGC×93 09:30 $10.65 → close $11.19 +50.22; HAFN×107 09:30 $9.32 → close $9.38 +6.42; INSP×14 09:30 $69.88 → close $73.00 +43.68; FRO×20 09:30 $48.05 → close $49.21 +23.20; GME×47 09:30 $21.04 → close $21.15 +5.17 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.41 | ▼ 09:30 equity $8,075.25 vs yday $8,085.77 (-10.52) | 09:30 open · cash $64.41 (unchanged overnight, no fees) · equity $8,075.25 vs prior close $8,085.77 (-10.52) · 8 name(s) re-marked at the open (per-name table). AMTX×490 yday $2.01 → 09:30 $2.01 +0.00; CLOV×210 yday $4.82 → 09:30 $4.82 +0.00; BAK×471 yday $2.08 → 09:30 $2.05 -14.13; TJGC×93 yday $11.19 → 09:30 $11.24 +5.11; HAFN×107 yday $9.38 → 09:30 $9.35 -3.21; INSP×14 yday $73.00 → 09:30 $72.14 -12.04; FRO×20 yday $49.21 → 09:30 $50.25 +20.80; GME×47 yday $21.15 → 09:30 $21.00 -7.05 | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 490 | $2.01 | $6.41 | $-27.43 | $1,042.90 | ▼ -27.43 after sell → book $8,068.84; vs 09:30 mark -6.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 210 | $4.82 | $2.75 | $+9.24 | $2,052.34 | ▲ +9.24 after sell → book $8,066.09; vs 09:30 mark -2.75 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 471 | $2.05 | $6.16 | $-45.21 | $3,011.73 | ▼ -45.21 after sell → book $8,059.92; vs 09:30 mark -6.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TJGC` | 93 | $11.24 | $2.29 | $+50.77 | $4,055.22 | ▲ +50.77 after sell → book $8,057.63; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟡 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-14 09:30 ET | **SELL** | `HAFN` | 107 | $9.35 | $2.34 | $-1.44 | $5,053.33 | ▼ -1.44 after sell → book $8,055.29; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INSP` | 14 | $72.14 | $2.05 | $+27.56 | $6,061.24 | ▲ +27.56 after sell → book $8,053.24; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🔴 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,061.24 | ▲ close $8,087.78 vs 09:30 $8,075.25 (session +34.54) | 16:00 close · cash $6,061.24 · equity $8,087.78 vs 09:30 $8,075.25 (+12.53; session marks +34.54) · 2 name(s) marked open→close (per-name table). FRO×20 09:30 $50.25 → close $50.52 +5.40; GME×47 09:30 $21.00 → close $21.62 +29.14 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,061.24 | ▲ 09:30 equity $8,096.41 vs yday $8,087.78 (+8.63) | 09:30 open · cash $6,061.24 (unchanged overnight, no fees) · equity $8,096.41 vs prior close $8,087.78 (+8.63) · 2 name(s) re-marked at the open (per-name table). FRO×20 yday $50.52 → 09:30 $51.21 +13.80; GME×47 yday $21.62 → 09:30 $21.51 -5.17 | — |
| 2026-09-15 09:30 ET | **SELL** | `GME` | 47 | $21.51 | $2.15 | $+17.81 | $7,070.06 | ▲ +17.81 after sell → book $8,094.26; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,070.06 | ▲ close $8,101.86 vs 09:30 $8,096.41 (session +7.60) | 16:00 close · cash $7,070.06 · equity $8,101.86 vs 09:30 $8,096.41 (+5.45; session marks +7.60) · 1 name(s) marked open→close (per-name table). FRO×20 09:30 $51.21 → close $51.59 +7.60 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,070.06 | ▲ 09:30 equity $8,120.46 vs yday $8,101.86 (+18.60) | 09:30 open · cash $7,070.06 (unchanged overnight, no fees) · equity $8,120.46 vs prior close $8,101.86 (+18.60) · 1 name(s) re-marked at the open (per-name table). FRO×20 yday $51.59 → 09:30 $52.52 +18.60 | — |
| 2026-09-16 09:30 ET | **SELL** | `FRO` | 20 | $52.52 | $2.07 | $+85.28 | $8,118.39 | ▲ +85.28 after sell → book $8,118.39; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `META` | 1 | $679.91 | $1.99 | — | $7,436.49 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+9.3; leftover $1014.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 25 | $39.99 | $2.06 | — | $6,434.67 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.3; leftover $1014.80 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `KR` | 16 | $61.93 | $2.04 | — | $5,441.75 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.8; leftover $1014.80 | join🔴 sector🔴 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ATRC` | 18 | $55.66 | $2.04 | — | $4,437.83 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.6; leftover $1014.80 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-16 09:30 ET | **BUY** | `APA` | 21 | $46.44 | $2.05 | — | $3,460.54 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.9; leftover $1014.80 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 5 | $189.17 | $2.00 | — | $2,512.68 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.9; leftover $1014.80 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `TK` | 71 | $14.26 | $2.20 | — | $1,498.02 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+6.8; leftover $1014.80 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `KGS` | 17 | $58.00 | $2.04 | — | $509.98 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+9.1; leftover $1014.80 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 catal🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $509.98 | ▼ close $8,023.50 vs 09:30 $8,120.46 (session -78.45) | 16:00 close · cash $509.98 · equity $8,023.50 vs 09:30 $8,120.46 (-96.96; session marks -78.45) · 8 name(s) marked open→close (per-name table). META×1 09:30 $679.91 → close $673.31 -6.60; SM×25 09:30 $39.99 → close $38.16 -45.75; KR×16 09:30 $61.93 → close $61.14 -12.64; ATRC×18 09:30 $55.66 → close $57.14 +26.64; APA×21 09:30 $46.44 → close $44.79 -34.65; QCOM×5 09:30 $189.17 → close $184.84 -21.65; TK×71 09:30 $14.26 → close $14.39 +9.23; KGS×17 09:30 $58.00 → close $58.41 +6.97 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $509.98 | ▲ 09:30 equity $8,064.83 vs yday $8,023.50 (+41.33) | 09:30 open · cash $509.98 (unchanged overnight, no fees) · equity $8,064.83 vs prior close $8,023.50 (+41.33) · 8 name(s) re-marked at the open (per-name table). META×1 yday $673.31 → 09:30 $682.44 +9.13; SM×25 yday $38.16 → 09:30 $37.57 -14.75; KR×16 yday $61.14 → 09:30 $61.02 -1.92; ATRC×18 yday $57.14 → 09:30 $57.96 +14.76; APA×21 yday $44.79 → 09:30 $44.63 -3.36; QCOM×5 yday $184.84 → 09:30 $190.35 +27.55; TK×71 yday $14.39 → 09:30 $14.41 +1.42; KGS×17 yday $58.41 → 09:30 $58.91 +8.50 | — |
| 2026-09-17 09:30 ET | **SELL** | `META` | 1 | $682.44 | $2.01 | $-1.48 | $1,190.40 | ▼ -1.48 after sell → book $8,062.81; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 25 | $37.57 | $2.08 | $-64.65 | $2,127.57 | ▼ -64.65 after sell → book $8,060.73; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `KR` | 16 | $61.02 | $2.06 | $-18.66 | $3,101.83 | ▼ -18.66 after sell → book $8,058.67; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ATRC` | 18 | $57.96 | $2.06 | $+37.29 | $4,143.05 | ▲ +37.29 after sell → book $8,056.61; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `APA` | 21 | $44.63 | $2.07 | $-42.14 | $5,078.20 | ▼ -42.14 after sell → book $8,054.53; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 5 | $190.35 | $2.02 | $+1.87 | $6,027.93 | ▲ +1.87 after sell → book $8,052.51; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 132 | $7.59 | $2.39 | — | $5,023.66 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1004.65 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 418 | $2.40 | $5.39 | — | $4,015.07 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1004.65 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `QTRX` | 341 | $2.94 | $4.40 | — | $3,008.13 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,ohlc_hot; 🔵; ret5=+9.8; leftover $1004.65 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `BNC` | 199 | $5.03 | $2.59 | — | $2,004.57 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.4; leftover $1004.65 | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `SFL` | 74 | $13.55 | $2.21 | — | $999.66 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.8; leftover $1004.65 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-17 09:30 ET | **BUY** | `PUMP` | 96 | $10.31 | $2.28 | — | $7.62 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+5.9; leftover $1004.65 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.62 | ▲ close $8,212.71 vs 09:30 $8,064.83 (session +179.46) | 16:00 close · cash $7.62 · equity $8,212.71 vs 09:30 $8,064.83 (+147.88; session marks +179.46) · 8 name(s) marked open→close (per-name table). TK×71 09:30 $14.41 → close $14.67 +18.46; KGS×17 09:30 $58.91 → close $58.28 -10.71; PGEN×132 09:30 $7.59 → close $7.87 +36.96; SABR×418 09:30 $2.40 → close $2.32 -33.44; QTRX×341 09:30 $2.94 → close $3.12 +61.38; BNC×199 09:30 $5.03 → close $5.42 +77.61; SFL×74 09:30 $13.55 → close $13.75 +14.80; PUMP×96 09:30 $10.31 → close $10.46 +14.40 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.62 | ▲ 09:30 equity $8,294.19 vs yday $8,212.71 (+81.48) | 09:30 open · cash $7.62 (unchanged overnight, no fees) · equity $8,294.19 vs prior close $8,212.71 (+81.48) · 8 name(s) re-marked at the open (per-name table). TK×71 yday $14.67 → 09:30 $14.60 -4.97; KGS×17 yday $58.28 → 09:30 $58.38 +1.70; PGEN×132 yday $7.87 → 09:30 $7.98 +14.52; SABR×418 yday $2.32 → 09:30 $2.29 -12.54; QTRX×341 yday $3.12 → 09:30 $3.12 +0.00; BNC×199 yday $5.42 → 09:30 $5.83 +81.59; SFL×74 yday $13.75 → 09:30 $13.74 -0.74; PUMP×96 yday $10.46 → 09:30 $10.48 +1.92 | — |
| 2026-09-18 09:30 ET | **SELL** | `TK` | 71 | $14.60 | $2.22 | $+19.71 | $1,042.00 | ▲ +19.71 after sell → book $8,291.97; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `KGS` | 17 | $58.38 | $2.06 | $+2.36 | $2,032.40 | ▲ +2.36 after sell → book $8,289.91; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 132 | $7.98 | $2.42 | $+46.68 | $3,083.34 | ▲ +46.68 after sell → book $8,287.49; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 418 | $2.29 | $5.47 | $-56.84 | $4,035.09 | ▼ -56.84 after sell → book $8,282.02; vs 09:30 mark -5.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `QTRX` | 341 | $3.12 | $4.47 | $+52.52 | $5,094.54 | ▲ +52.52 after sell → book $8,277.55; vs 09:30 mark -4.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SFL` | 74 | $13.74 | $2.23 | $+9.61 | $6,109.07 | ▲ +9.61 after sell → book $8,275.32; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PUMP` | 96 | $10.48 | $2.30 | $+11.74 | $7,112.85 | ▲ +11.74 after sell → book $8,273.02; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 4 | $547.37 | $2.00 | — | $4,921.36 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.2; leftover $2370.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `SYM` | 53 | $44.70 | $2.15 | — | $2,550.11 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.5; leftover $2370.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 113 | $20.91 | $2.33 | — | $184.96 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2370.95 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $184.96 | ▼ close $8,228.90 vs 09:30 $8,294.19 (session -37.64) | 16:00 close · cash $184.96 · equity $8,228.90 vs 09:30 $8,294.19 (-65.29; session marks -37.64) · 4 name(s) marked open→close (per-name table). BNC×199 09:30 $5.83 → close $5.98 +29.85; AMD×4 09:30 $547.37 → close $559.82 +49.80; SYM×53 09:30 $44.70 → close $41.89 -148.93; TH×113 09:30 $20.91 → close $21.19 +31.64 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $184.96 | ▲ 09:30 equity $8,491.77 vs yday $8,228.90 (+262.87) | 09:30 open · cash $184.96 (unchanged overnight, no fees) · equity $8,491.77 vs prior close $8,228.90 (+262.87) · 4 name(s) re-marked at the open (per-name table). BNC×199 yday $5.98 → 09:30 $6.42 +86.56; AMD×4 yday $559.82 → 09:30 $583.88 +96.24; SYM×53 yday $41.89 → 09:30 $42.42 +28.09; TH×113 yday $21.19 → 09:30 $21.65 +51.98 | — |
| 2026-09-21 09:30 ET | **SELL** | `BNC` | 199 | $6.42 | $2.63 | $+270.40 | $1,458.91 | ▲ +270.40 after sell → book $8,489.14; vs 09:30 mark -2.63 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SYM` | 53 | $42.42 | $2.18 | $-125.17 | $3,704.99 | ▼ -125.17 after sell → book $8,486.96; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 113 | $21.65 | $2.37 | $+78.92 | $6,149.08 | ▲ +78.92 after sell → book $8,484.60; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 65 | $13.47 | $2.19 | — | $5,271.02 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $878.44 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `MXL` | 10 | $83.53 | $2.02 | — | $4,433.70 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.8; leftover $878.44 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `FORM` | 7 | $123.00 | $2.01 | — | $3,570.68 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+3.0; leftover $878.44 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `COHR` | 2 | $326.48 | $2.00 | — | $2,915.73 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+3.9; leftover $878.44 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `ASST` | 27 | $31.64 | $2.07 | — | $2,059.38 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.9; leftover $878.44 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `SHMD` | 234 | $3.75 | $3.02 | — | $1,178.86 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+9.2; leftover $878.44 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `TRMD` | 23 | $37.47 | $2.06 | — | $314.99 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.0; leftover $878.44 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $314.99 | ▼ close $8,467.87 vs 09:30 $8,491.77 (session -1.36) | 16:00 close · cash $314.99 · equity $8,467.87 vs 09:30 $8,491.77 (-23.90; session marks -1.36) · 8 name(s) marked open→close (per-name table). AMD×4 09:30 $583.88 → close $615.52 +126.56; BTDR×65 09:30 $13.47 → close $13.14 -21.77; MXL×10 09:30 $83.53 → close $85.98 +24.50; FORM×7 09:30 $123.00 → close $119.31 -25.83; COHR×2 09:30 $326.48 → close $321.52 -9.92; ASST×27 09:30 $31.64 → close $30.33 -35.37; SHMD×234 09:30 $3.75 → close $3.53 -51.48; TRMD×23 09:30 $37.47 → close $37.12 -8.05 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $314.99 | ▼ 09:30 equity $8,381.80 vs yday $8,467.87 (-86.07) | 09:30 open · cash $314.99 (unchanged overnight, no fees) · equity $8,381.80 vs prior close $8,467.87 (-86.07) · 8 name(s) re-marked at the open (per-name table). AMD×4 yday $615.52 → 09:30 $606.57 -35.80; BTDR×65 yday $13.14 → 09:30 $13.14 +0.00; MXL×10 yday $85.98 → 09:30 $85.98 +0.00; FORM×7 yday $119.31 → 09:30 $119.31 +0.00; COHR×2 yday $321.52 → 09:30 $310.29 -22.46; ASST×27 yday $30.33 → 09:30 $29.30 -27.81; SHMD×234 yday $3.53 → 09:30 $3.53 +0.00; TRMD×23 yday $37.12 → 09:30 $37.12 +0.00 | — |
| 2026-09-22 09:30 ET | **SELL** | `AMD` | 4 | $606.57 | $2.03 | $+232.77 | $2,739.24 | ▲ +232.77 after sell → book $8,379.77; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `COHR` | 2 | $310.29 | $2.02 | $-36.39 | $3,357.80 | ▼ -36.39 after sell → book $8,377.75; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `ASST` | 27 | $29.30 | $2.09 | $-67.34 | $4,146.81 | ▼ -67.34 after sell → book $8,375.66; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,146.81 | ▲ close $8,375.66 vs 09:30 $8,381.80 (session +0.00) | 16:00 close · cash $4,146.81 · equity $8,375.66 vs 09:30 $8,381.80 (-6.14; session marks +0.00) · 5 name(s) marked open→close (per-name table). BTDR×65 09:30 $13.14 → close $13.14 +0.00; MXL×10 09:30 $85.98 → close $85.98 +0.00; FORM×7 09:30 $119.31 → close $119.31 +0.00; SHMD×234 09:30 $3.53 → close $3.53 +0.00; TRMD×23 09:30 $37.12 → close $37.12 +0.00 | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,146.81 | ▼ 09:30 equity $8,370.67 vs yday $8,375.66 (-4.99) | 09:30 open · cash $4,146.81 (unchanged overnight, no fees) · equity $8,370.67 vs prior close $8,375.66 (-4.99) · 5 name(s) re-marked at the open (per-name table). BTDR×65 yday $13.14 → 09:30 $12.84 -19.50; MXL×10 yday $85.98 → 09:30 $86.57 +5.90; FORM×7 yday $119.31 → 09:30 $125.39 +42.56; SHMD×234 yday $3.53 → 09:30 $3.61 +18.72; TRMD×23 yday $37.12 → 09:30 $34.83 -52.67 | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 65 | $12.84 | $2.21 | $-45.67 | $4,979.21 | ▼ -45.67 after sell → book $8,368.47; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MXL` | 10 | $86.57 | $2.04 | $+26.34 | $5,842.87 | ▲ +26.34 after sell → book $8,366.43; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FORM` | 7 | $125.39 | $2.03 | $+12.69 | $6,718.56 | ▲ +12.69 after sell → book $8,364.39; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-23 09:30 ET | **SELL** | `SHMD` | 234 | $3.61 | $3.07 | $-38.85 | $7,560.24 | ▼ -38.85 after sell → book $8,361.33; vs 09:30 mark -3.06 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TRMD` | 23 | $34.83 | $2.08 | $-64.86 | $8,359.25 | ▼ -64.86 after sell → book $8,359.25; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 354 | $3.93 | $4.57 | — | $6,963.46 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1393.21 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `NTSK` | 75 | $18.57 | $2.21 | — | $5,568.12 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.2; leftover $1393.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `ZS` | 6 | $213.00 | $2.01 | — | $4,288.11 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.0; leftover $1393.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `HIMS` | 45 | $30.40 | $2.12 | — | $2,917.99 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.3; leftover $1393.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 34 | $40.00 | $2.09 | — | $1,555.90 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+6.7; leftover $1393.21 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `OPRT` | 169 | $8.23 | $2.50 | — | $162.53 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.7; leftover $1393.21 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $162.53 | ▼ close $8,250.97 vs 09:30 $8,370.67 (session -92.77) | 16:00 close · cash $162.53 · equity $8,250.97 vs 09:30 $8,370.67 (-119.70; session marks -92.77) · 6 name(s) marked open→close (per-name table). INDP×354 09:30 $3.93 → close $3.77 -56.64; NTSK×75 09:30 $18.57 → close $18.57 -0.37; ZS×6 09:30 $213.00 → close $214.45 +8.70; HIMS×45 09:30 $30.40 → close $28.39 -90.45; BLSH×34 09:30 $40.00 → close $40.11 +3.74; OPRT×169 09:30 $8.23 → close $8.48 +42.25 | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $162.53 | ▼ 09:30 equity $8,192.07 vs yday $8,250.97 (-58.90) | 09:30 open · cash $162.53 (unchanged overnight, no fees) · equity $8,192.07 vs prior close $8,250.97 (-58.90) · 6 name(s) re-marked at the open (per-name table). INDP×354 yday $3.77 → 09:30 $3.77 +0.00; NTSK×75 yday $18.57 → 09:30 $18.50 -5.25; ZS×6 yday $214.45 → 09:30 $213.47 -5.85; HIMS×45 yday $28.39 → 09:30 $28.00 -17.55; BLSH×34 yday $40.11 → 09:30 $39.27 -28.56; OPRT×169 yday $8.48 → 09:30 $8.47 -1.69 | — |
| 2026-09-24 09:30 ET | **SELL** | `INDP` | 354 | $3.77 | $4.64 | $-65.84 | $1,492.47 | ▼ -65.84 after sell → book $8,187.43; vs 09:30 mark -4.64 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-24 09:30 ET | **SELL** | `ZS` | 6 | $213.47 | $2.03 | $-1.19 | $2,771.30 | ▼ -1.19 after sell → book $8,185.41; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-24 09:30 ET | **SELL** | `HIMS` | 45 | $28.00 | $2.15 | $-112.27 | $4,029.15 | ▼ -112.27 after sell → book $8,183.26; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 34 | $39.27 | $2.11 | $-29.02 | $5,362.22 | ▼ -29.02 after sell → book $8,181.15; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,362.22 | ▼ close $8,157.67 vs 09:30 $8,192.07 (session -23.48) | 16:00 close · cash $5,362.22 · equity $8,157.67 vs 09:30 $8,192.07 (-34.40; session marks -23.48) · 2 name(s) marked open→close (per-name table). NTSK×75 09:30 $18.50 → close $18.57 +5.25; OPRT×169 09:30 $8.47 → close $8.30 -28.73 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BETA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `U` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VSTM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMTX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PSX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RCKT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GWRE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TII` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FNKO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ARCT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ZETA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TSLA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SIGA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VIR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AGRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CSAN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `LAND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `LAND` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `XRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `STX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CDZI` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TJGC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLMT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `QRVO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CRDL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `KGS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `PUMP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `PGNY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `KGS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PUMP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `META` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOVA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `MXL` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FORM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SHMD` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TRMD` | no_price | no 09:30 open — carry |
| 2026-09-22 | `NTSK` | no_price | no 09:30 open |
| 2026-09-22 | `ZS` | no_price | no 09:30 open |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RNG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RPD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AVT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DDOG` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `NTSK` | 75 | 2026-09-23 @ $18.57 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.2; leftover $1393.21 |
| `OPRT` | 169 | 2026-09-23 @ $8.23 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.7; leftover $1393.21 |
