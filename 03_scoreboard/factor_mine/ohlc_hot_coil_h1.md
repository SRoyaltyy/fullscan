# Factor mine action — `ohlc_hot_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · hot list ∩ not exploded

Cash book **-20.10%** ($7,990) · signal-only (no cash/fees) was -8.74%. Starts YES **1/21**. Fills 76 · skips 44 · realized $-1956.77.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $261.77.

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
| 2026-08-27 | `INO` | 1395 | $1.30 | $1.29 | -13.95 | — | +0.00 | -13.95 | +55.80 | — |
| 2026-08-27 | `CRDL` | 3311 | $2.14 | $2.09 | -165.55 | — | +0.00 | -165.55 | +198.66 | — |
| 2026-08-28 | `MRNA` | 21 | — | $137.19 | +0.00 | $137.99 | +16.80 | +16.80 | +0.00 | +16.80 |
| 2026-08-28 | `TH` | 151 | — | $19.00 | +0.00 | $18.55 | -67.95 | -67.95 | +0.00 | -67.95 |
| 2026-08-28 | `FSM` | 224 | — | $12.84 | +0.00 | $12.26 | -129.92 | -129.92 | +0.00 | -129.92 |
| 2026-08-31 | `MRNA` | 21 | $137.99 | $134.10 | -81.69 | — | +0.00 | -81.69 | -64.89 | — |
| 2026-08-31 | `TH` | 151 | $18.55 | $18.12 | -64.18 | — | +0.00 | -64.18 | -132.12 | — |
| 2026-08-31 | `FSM` | 224 | $12.26 | $12.26 | +0.00 | — | +0.00 | +0.00 | -129.92 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `CABA` | 458 | — | $3.63 | +0.00 | $3.48 | -68.70 | -68.70 | +0.00 | -68.70 |
| 2026-09-03 | `ARCT` | 99 | — | $16.77 | +0.00 | $15.56 | -119.79 | -119.79 | +0.00 | -119.79 |
| 2026-09-03 | `EBS` | 253 | — | $6.56 | +0.00 | $6.23 | -83.49 | -83.49 | +0.00 | -83.49 |
| 2026-09-03 | `GALT` | 369 | — | $4.50 | +0.00 | $4.35 | -55.35 | -55.35 | +0.00 | -55.35 |
| 2026-09-03 | `CTVA` | 18 | — | $90.24 | +0.00 | $88.62 | -29.16 | -29.16 | +0.00 | -29.16 |
| 2026-09-04 | `CABA` | 458 | $3.48 | $3.46 | -9.16 | — | +0.00 | -9.16 | -77.86 | — |
| 2026-09-04 | `ARCT` | 99 | $15.56 | $15.61 | +4.95 | — | +0.00 | +4.95 | -114.84 | — |
| 2026-09-04 | `EBS` | 253 | $6.23 | $6.26 | +7.59 | — | +0.00 | +7.59 | -75.90 | — |
| 2026-09-04 | `GALT` | 369 | $4.35 | $4.33 | -7.38 | — | +0.00 | -7.38 | -62.73 | — |
| 2026-09-04 | `CTVA` | 18 | $88.62 | $87.64 | -17.64 | — | +0.00 | -17.64 | -46.80 | — |
| 2026-09-04 | `USDE` | 125 | — | $7.87 | +0.00 | $7.93 | +7.50 | +7.50 | +0.00 | +7.50 |
| 2026-09-04 | `GORO` | 250 | — | $3.95 | +0.00 | $4.15 | +50.00 | +50.00 | +0.00 | +50.00 |
| 2026-09-04 | `CRCL` | 10 | — | $97.98 | +0.00 | $102.05 | +40.70 | +40.70 | +0.00 | +40.70 |
| 2026-09-04 | `MSTR` | 7 | — | $137.35 | +0.00 | $142.80 | +38.15 | +38.15 | +0.00 | +38.15 |
| 2026-09-04 | `BLSH` | 28 | — | $34.69 | +0.00 | $36.00 | +36.68 | +36.68 | +0.00 | +36.68 |
| 2026-09-04 | `ZETA` | 30 | — | $32.65 | +0.00 | $31.35 | -39.00 | -39.00 | +0.00 | -39.00 |
| 2026-09-04 | `HAFN` | 110 | — | $8.94 | +0.00 | $9.22 | +30.80 | +30.80 | +0.00 | +30.80 |
| 2026-09-04 | `BE` | 4 | — | $236.82 | +0.00 | $252.87 | +64.20 | +64.20 | +0.00 | +64.20 |
| 2026-09-08 | `USDE` | 125 | $7.93 | $7.76 | -21.25 | — | +0.00 | -21.25 | -13.75 | — |
| 2026-09-08 | `GORO` | 250 | $4.15 | $4.13 | -5.00 | — | +0.00 | -5.00 | +45.00 | — |
| 2026-09-08 | `CRCL` | 10 | $102.05 | $100.65 | -14.00 | — | +0.00 | -14.00 | +26.70 | — |
| 2026-09-08 | `MSTR` | 7 | $142.80 | $137.62 | -36.26 | — | +0.00 | -36.26 | +1.89 | — |
| 2026-09-08 | `BLSH` | 28 | $36.00 | $35.90 | -2.80 | — | +0.00 | -2.80 | +33.88 | — |
| 2026-09-08 | `ZETA` | 30 | $31.35 | $31.08 | -8.10 | — | +0.00 | -8.10 | -47.10 | — |
| 2026-09-08 | `HAFN` | 110 | $9.22 | $8.81 | -45.10 | $8.96 | +16.50 | -28.60 | -14.30 | +2.20 |
| 2026-09-08 | `BE` | 4 | $252.87 | $267.76 | +59.56 | — | +0.00 | +59.56 | +123.76 | — |
| 2026-09-09 | `HAFN` | 110 | $8.96 | $9.00 | +4.40 | — | +0.00 | +4.40 | +6.60 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `BAK` | 474 | — | $2.12 | +0.00 | $2.08 | -18.96 | -18.96 | +0.00 | -18.96 |
| 2026-09-11 | `TJGC` | 94 | — | $10.65 | +0.00 | $11.19 | +50.76 | +50.76 | +0.00 | +50.76 |
| 2026-09-11 | `HAFN` | 107 | — | $9.32 | +0.00 | $9.38 | +6.42 | +6.42 | +0.00 | +6.42 |
| 2026-09-11 | `LITE` | 1 | — | $945.60 | +0.00 | $927.03 | -18.57 | -18.57 | +0.00 | -18.57 |
| 2026-09-11 | `INSP` | 14 | — | $69.88 | +0.00 | $73.00 | +43.68 | +43.68 | +0.00 | +43.68 |
| 2026-09-11 | `FRO` | 20 | — | $48.05 | +0.00 | $49.21 | +23.20 | +23.20 | +0.00 | +23.20 |
| 2026-09-11 | `STX` | 1 | — | $869.42 | +0.00 | $830.17 | -39.25 | -39.25 | +0.00 | -39.25 |
| 2026-09-11 | `CDZI` | 253 | — | $3.96 | +0.00 | $3.65 | -78.43 | -78.43 | +0.00 | -78.43 |

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
| 2026-08-27 | — | $1.45 | INO×1395, CRDL×3311 | $8,720.99 | -179.50 | +0.00 | — | INO, CRDL | $8,659.43 | $8,659.43 | — |
| 2026-08-28 | +0.75 | $8,659.43 | — | $8,659.43 | +0.00 | -181.07 | MRNA, TH, FSM | — | $25.90 | $8,470.98 | MRNA×21, TH×151, FSM×224 |
| 2026-08-31 | -5.85 | $25.90 | MRNA×21, TH×151, FSM×224 | $8,325.11 | -145.87 | +0.00 | — | MRNA, TH, FSM | $8,317.59 | $8,317.59 | — |
| 2026-09-01 | -6.30 | $8,317.59 | — | $8,317.59 | -0.00 | +0.00 | — | — | $8,317.59 | $8,317.59 | — |
| 2026-09-02 | -3.83 | $8,317.59 | — | $8,317.59 | -0.00 | +0.00 | — | — | $8,317.59 | $8,317.59 | — |
| 2026-09-03 | -0.90 | $8,317.59 | — | $8,317.59 | -0.00 | -356.49 | CABA, ARCT, EBS, GALT, CTVA | — | $32.05 | $7,942.83 | CABA×458, ARCT×99, EBS×253, GALT×369, CTVA×18 |
| 2026-09-04 | +2.25 | $32.05 | CABA×458, ARCT×99, EBS×253, GALT×369, CTVA×18 | $7,921.19 | -21.64 | +229.03 | USDE, GORO, CRCL, MSTR, BLSH, ZETA, HAFN, BE | CABA, ARCT, EBS, GALT, CTVA | $90.56 | $8,113.59 | USDE×125, GORO×250, CRCL×10, MSTR×7, BLSH×28, ZETA×30, HAFN×110, BE×4 |
| 2026-09-08 | -11.47 | $90.56 | USDE×125, GORO×250, CRCL×10, MSTR×7, BLSH×28, ZETA×30, HAFN×110, BE×4 | $8,040.64 | -72.95 | +16.50 | — | USDE, GORO, CRCL, MSTR, BLSH, ZETA, BE | $7,055.59 | $8,041.19 | HAFN×110 |
| 2026-09-09 | -13.95 | $7,055.59 | HAFN×110 | $8,045.59 | +4.40 | +0.00 | — | HAFN | $8,043.24 | $8,043.24 | — |
| 2026-09-10 | -13.28 | $8,043.24 | — | $8,043.24 | -0.00 | +0.00 | — | — | $8,043.24 | $8,043.24 | — |
| 2026-09-11 | +0.50 | $8,043.24 | — | $8,043.24 | -0.00 | -31.15 | BAK, TJGC, HAFN, LITE, INSP, FRO, STX, CDZI | — | $261.77 | $7,990.06 | BAK×474, TJGC×94, HAFN×107, LITE×1, INSP×14, FRO×20, STX×1, CDZI×253 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 605 | $16.50 | $7.80 | — | $9.70 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $10000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟡 vol🟢 buy🟡 |
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
| 2026-08-27 09:30 ET | **SELL** | `INO` | 1395 | $1.29 | $18.24 | $+19.56 | $1,782.76 | ▲ +19.56 after sell → book $8,702.75; vs 09:30 mark -18.24 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 3311 | $2.09 | $43.32 | $+112.63 | $8,659.43 | ▲ +112.63 after sell → book $8,659.43; vs 09:30 mark -43.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,659.43 | ▲ close $8,659.43 vs 09:30 $8,720.99 (session +0.00) | 16:00 close · cash $8,659.43 · no lots left · equity $8,659.43. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,659.43 | ▲ 09:30 equity $8,659.43 vs yday $8,659.43 (+0.00) | 09:30 open · cash $8,659.43 · no holdings · equity $8,659.43 vs prior close $8,659.43 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `MRNA` | 21 | $137.19 | $2.05 | — | $5,776.39 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.1; leftover $2886.48 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 151 | $19.00 | $2.44 | — | $2,904.95 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.5; leftover $2886.48 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FSM` | 224 | $12.84 | $2.89 | — | $25.90 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.6; leftover $2886.48 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.90 | ▼ close $8,470.98 vs 09:30 $8,659.43 (session -181.07) | 16:00 close · cash $25.90 · equity $8,470.98 vs 09:30 $8,659.43 (-188.45; session marks -181.07) · 3 name(s) marked open→close (per-name table). MRNA×21 09:30 $137.19 → close $137.99 +16.80; TH×151 09:30 $19.00 → close $18.55 -67.95; FSM×224 09:30 $12.84 → close $12.26 -129.92 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.90 | ▼ 09:30 equity $8,325.11 vs yday $8,470.98 (-145.87) | 09:30 open · cash $25.90 (unchanged overnight, no fees) · equity $8,325.11 vs prior close $8,470.98 (-145.87) · 3 name(s) re-marked at the open (per-name table). MRNA×21 yday $137.99 → 09:30 $134.10 -81.69; TH×151 yday $18.55 → 09:30 $18.12 -64.18; FSM×224 yday $12.26 → 09:30 $12.26 +0.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 21 | $134.10 | $2.09 | $-69.03 | $2,839.91 | ▼ -69.03 after sell → book $8,323.03; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 151 | $18.12 | $2.49 | $-137.06 | $5,574.30 | ▼ -137.06 after sell → book $8,320.54; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FSM` | 224 | $12.26 | $2.95 | $-135.76 | $8,317.59 | ▼ -135.76 after sell → book $8,317.59; vs 09:30 mark -2.95 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,317.59 | ▲ close $8,317.59 vs 09:30 $8,325.11 (session +0.00) | 16:00 close · cash $8,317.59 · no lots left · equity $8,317.59. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,317.59 | ▲ 09:30 equity $8,317.59 vs yday $8,317.59 (-0.00) | 09:30 open · cash $8,317.59 · no holdings · equity $8,317.59 vs prior close $8,317.59 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,317.59 | ▲ close $8,317.59 vs 09:30 $8,317.59 (session +0.00) | 16:00 close · cash $8,317.59 · no lots left · equity $8,317.59. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,317.59 | ▲ 09:30 equity $8,317.59 vs yday $8,317.59 (-0.00) | 09:30 open · cash $8,317.59 · no holdings · equity $8,317.59 vs prior close $8,317.59 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,317.59 | ▲ close $8,317.59 vs 09:30 $8,317.59 (session +0.00) | 16:00 close · cash $8,317.59 · no lots left · equity $8,317.59. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,317.59 | ▲ 09:30 equity $8,317.59 vs yday $8,317.59 (-0.00) | 09:30 open · cash $8,317.59 · no holdings · equity $8,317.59 vs prior close $8,317.59 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 458 | $3.63 | $5.91 | — | $6,649.14 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1663.52 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 99 | $16.77 | $2.29 | — | $4,986.62 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1663.52 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EBS` | 253 | $6.56 | $3.26 | — | $3,323.68 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ⚪; ret5=+8.2; leftover $1663.52 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GALT` | 369 | $4.50 | $4.76 | — | $1,658.42 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.0; leftover $1663.52 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTVA` | 18 | $90.24 | $2.04 | — | $32.05 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6; leftover $1663.52 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.05 | ▼ close $7,942.83 vs 09:30 $8,317.59 (session -356.49) | 16:00 close · cash $32.05 · equity $7,942.83 vs 09:30 $8,317.59 (-374.76; session marks -356.49) · 5 name(s) marked open→close (per-name table). CABA×458 09:30 $3.63 → close $3.48 -68.70; ARCT×99 09:30 $16.77 → close $15.56 -119.79; EBS×253 09:30 $6.56 → close $6.23 -83.49; GALT×369 09:30 $4.50 → close $4.35 -55.35; CTVA×18 09:30 $90.24 → close $88.62 -29.16 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.05 | ▼ 09:30 equity $7,921.19 vs yday $7,942.83 (-21.64) | 09:30 open · cash $32.05 (unchanged overnight, no fees) · equity $7,921.19 vs prior close $7,942.83 (-21.64) · 5 name(s) re-marked at the open (per-name table). CABA×458 yday $3.48 → 09:30 $3.46 -9.16; ARCT×99 yday $15.56 → 09:30 $15.61 +4.95; EBS×253 yday $6.23 → 09:30 $6.26 +7.59; GALT×369 yday $4.35 → 09:30 $4.33 -7.38; CTVA×18 yday $88.62 → 09:30 $87.64 -17.64 | — |
| 2026-09-04 09:30 ET | **SELL** | `CABA` | 458 | $3.46 | $6.00 | $-89.77 | $1,610.74 | ▼ -89.77 after sell → book $7,915.20; vs 09:30 mark -5.99 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 99 | $15.61 | $2.32 | $-119.44 | $3,153.81 | ▼ -119.44 after sell → book $7,912.88; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EBS` | 253 | $6.26 | $3.32 | $-82.48 | $4,734.27 | ▼ -82.48 after sell → book $7,909.56; vs 09:30 mark -3.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GALT` | 369 | $4.33 | $4.83 | $-72.32 | $6,327.21 | ▼ -72.32 after sell → book $7,904.73; vs 09:30 mark -4.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTVA` | 18 | $87.64 | $2.07 | $-50.91 | $7,902.66 | ▼ -50.91 after sell → book $7,902.66; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 125 | $7.87 | $2.37 | — | $6,916.55 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.7; leftover $987.83 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GORO` | 250 | $3.95 | $3.23 | — | $5,925.82 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+6.9; leftover $987.83 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CRCL` | 10 | $97.98 | $2.02 | — | $4,944.00 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.5; leftover $987.83 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 7 | $137.35 | $2.01 | — | $3,980.54 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+5.4; leftover $987.83 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BLSH` | 28 | $34.69 | $2.07 | — | $3,007.15 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+7.9; leftover $987.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ZETA` | 30 | $32.65 | $2.08 | — | $2,025.57 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.1; leftover $987.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 110 | $8.94 | $2.32 | — | $1,039.85 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+7.7; leftover $987.83 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 4 | $236.82 | $2.00 | — | $90.56 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+8.1; leftover $987.83 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.56 | ▲ close $8,113.59 vs 09:30 $7,921.19 (session +229.03) | 16:00 close · cash $90.56 · equity $8,113.59 vs 09:30 $7,921.19 (+192.40; session marks +229.03) · 8 name(s) marked open→close (per-name table). USDE×125 09:30 $7.87 → close $7.93 +7.50; GORO×250 09:30 $3.95 → close $4.15 +50.00; CRCL×10 09:30 $97.98 → close $102.05 +40.70; MSTR×7 09:30 $137.35 → close $142.80 +38.15; BLSH×28 09:30 $34.69 → close $36.00 +36.68; ZETA×30 09:30 $32.65 → close $31.35 -39.00; HAFN×110 09:30 $8.94 → close $9.22 +30.80; BE×4 09:30 $236.82 → close $252.87 +64.20 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.56 | ▼ 09:30 equity $8,040.64 vs yday $8,113.59 (-72.95) | 09:30 open · cash $90.56 (unchanged overnight, no fees) · equity $8,040.64 vs prior close $8,113.59 (-72.95) · 8 name(s) re-marked at the open (per-name table). USDE×125 yday $7.93 → 09:30 $7.76 -21.25; GORO×250 yday $4.15 → 09:30 $4.13 -5.00; CRCL×10 yday $102.05 → 09:30 $100.65 -14.00; MSTR×7 yday $142.80 → 09:30 $137.62 -36.26; BLSH×28 yday $36.00 → 09:30 $35.90 -2.80; ZETA×30 yday $31.35 → 09:30 $31.08 -8.10; HAFN×110 yday $9.22 → 09:30 $8.81 -45.10; BE×4 yday $252.87 → 09:30 $267.76 +59.56 | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 125 | $7.76 | $2.40 | $-18.51 | $1,058.17 | ▼ -18.51 after sell → book $8,038.25; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `GORO` | 250 | $4.13 | $3.28 | $+38.50 | $2,087.39 | ▲ +38.50 after sell → book $8,034.97; vs 09:30 mark -3.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `CRCL` | 10 | $100.65 | $2.04 | $+22.64 | $3,091.85 | ▲ +22.64 after sell → book $8,032.93; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MSTR` | 7 | $137.62 | $2.03 | $-2.15 | $4,053.16 | ▼ -2.15 after sell → book $8,030.90; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `BLSH` | 28 | $35.90 | $2.09 | $+29.71 | $5,056.27 | ▲ +29.71 after sell → book $8,028.81; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZETA` | 30 | $31.08 | $2.10 | $-51.28 | $5,986.57 | ▼ -51.28 after sell → book $8,026.71; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 4 | $267.76 | $2.02 | $+119.74 | $7,055.59 | ▲ +119.74 after sell → book $8,024.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,055.59 | ▲ close $8,041.19 vs 09:30 $8,040.64 (session +16.50) | 16:00 close · cash $7,055.59 · equity $8,041.19 vs 09:30 $8,040.64 (+0.55; session marks +16.50) · 1 name(s) marked open→close (per-name table). HAFN×110 09:30 $8.81 → close $8.96 +16.50 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,055.59 | ▲ 09:30 equity $8,045.59 vs yday $8,041.19 (+4.40) | 09:30 open · cash $7,055.59 (unchanged overnight, no fees) · equity $8,045.59 vs prior close $8,041.19 (+4.40) · 1 name(s) re-marked at the open (per-name table). HAFN×110 yday $8.96 → 09:30 $9.00 +4.40 | — |
| 2026-09-09 09:30 ET | **SELL** | `HAFN` | 110 | $9.00 | $2.35 | $+1.93 | $8,043.24 | ▲ +1.93 after sell → book $8,043.24; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,043.24 | ▲ close $8,043.24 vs 09:30 $8,045.59 (session +0.00) | 16:00 close · cash $8,043.24 · no lots left · equity $8,043.24. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,043.24 | ▲ 09:30 equity $8,043.24 vs yday $8,043.24 (-0.00) | 09:30 open · cash $8,043.24 · no holdings · equity $8,043.24 vs prior close $8,043.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,043.24 | ▲ close $8,043.24 vs 09:30 $8,043.24 (session +0.00) | 16:00 close · cash $8,043.24 · no lots left · equity $8,043.24. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,043.24 | ▲ 09:30 equity $8,043.24 vs yday $8,043.24 (-0.00) | 09:30 open · cash $8,043.24 · no holdings · equity $8,043.24 vs prior close $8,043.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 474 | $2.12 | $6.11 | — | $7,032.24 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1005.40 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `TJGC` | 94 | $10.65 | $2.27 | — | $6,028.87 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+4.3; leftover $1005.40 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `HAFN` | 107 | $9.32 | $2.31 | — | $5,029.32 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+5.4; leftover $1005.40 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `LITE` | 1 | $945.60 | $1.99 | — | $4,081.73 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1005.40 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `INSP` | 14 | $69.88 | $2.03 | — | $3,101.38 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.0; leftover $1005.40 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `FRO` | 20 | $48.05 | $2.05 | — | $2,138.33 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+6.6; leftover $1005.40 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `STX` | 1 | $869.42 | $1.99 | — | $1,266.92 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.2; leftover $1005.40 | join🟢 sector🟢 gen🟡 news🟡 digest🟡 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `CDZI` | 253 | $3.96 | $3.26 | — | $261.77 | — | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6; leftover $1005.40 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $261.77 | ▼ close $7,990.06 vs 09:30 $8,043.24 (session -31.15) | 16:00 close · cash $261.77 · equity $7,990.06 vs 09:30 $8,043.24 (-53.18; session marks -31.15) · 8 name(s) marked open→close (per-name table). BAK×474 09:30 $2.12 → close $2.08 -18.96; TJGC×94 09:30 $10.65 → close $11.19 +50.76; HAFN×107 09:30 $9.32 → close $9.38 +6.42; LITE×1 09:30 $945.60 → close $927.03 -18.57; INSP×14 09:30 $69.88 → close $73.00 +43.68; FRO×20 09:30 $48.05 → close $49.21 +23.20; STX×1 09:30 $869.42 → close $830.17 -39.25; CDZI×253 09:30 $3.96 → close $3.65 -78.43 | — |

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
| 2026-09-10 | `TJGC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LITE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `STX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CDZI` | hard_red | hard-red S=-13.28 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BAK` | 474 | 2026-09-11 @ $2.12 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1005.40 |
| `TJGC` | 94 | 2026-09-11 @ $10.65 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+4.3; leftover $1005.40 |
| `HAFN` | 107 | 2026-09-11 @ $9.32 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+5.4; leftover $1005.40 |
| `LITE` | 1 | 2026-09-11 @ $945.60 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1005.40 |
| `INSP` | 14 | 2026-09-11 @ $69.88 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.0; leftover $1005.40 |
| `FRO` | 20 | 2026-09-11 @ $48.05 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; ret5=+6.6; leftover $1005.40 |
| `STX` | 1 | 2026-09-11 @ $869.42 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.2; leftover $1005.40 |
| `CDZI` | 253 | 2026-09-11 @ $3.96 | hot list ∩ not exploded; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.6; leftover $1005.40 |
