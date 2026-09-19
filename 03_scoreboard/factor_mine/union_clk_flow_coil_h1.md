# Factor mine action — `union_clk_flow_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · Clock-B #8 flow-in + green + not extended

Cash book **-9.32%** ($9,068) · signal-only (no cash/fees) was -4.30%. Starts YES **7/26**. Fills 34 · skips 4 · realized $-931.75.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: Clock-B #8: prior flow-in, last bar green, not already extended.
- Must-not: Clock-B #5 long veto: extreme prior extension plus diminishing progress or a failed breakout.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
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
- **Gate** `clk_flow_coil=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,068.24.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `BSBR` | 1723 | — | $5.79 | +0.00 | $5.77 | -34.46 | -34.46 | +0.00 | -34.46 |
| 2026-08-17 | `BSBR` | 1723 | $5.77 | $5.78 | +17.23 | — | +0.00 | +17.23 | -17.23 | — |
| 2026-08-17 | `WBS` | 125 | — | $79.00 | +0.00 | $78.69 | -38.75 | -38.75 | +0.00 | -38.75 |
| 2026-08-18 | `WBS` | 125 | $78.69 | $78.52 | -21.25 | — | +0.00 | -21.25 | -60.00 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `FUTU` | 83 | — | $117.65 | +0.00 | $112.73 | -408.36 | -408.36 | +0.00 | -408.36 |
| 2026-08-21 | `FUTU` | 83 | $112.73 | $115.18 | +203.35 | — | +0.00 | +203.35 | -205.01 | — |
| 2026-08-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-25 | `AMX` | 203 | — | $23.80 | +0.00 | $23.75 | -10.15 | -10.15 | +0.00 | -10.15 |
| 2026-08-25 | `SAN` | 329 | — | $14.65 | +0.00 | $14.63 | -6.58 | -6.58 | +0.00 | -6.58 |
| 2026-08-26 | `AMX` | 203 | $23.75 | $23.75 | +0.00 | — | +0.00 | +0.00 | -10.15 | — |
| 2026-08-26 | `SAN` | 329 | $14.63 | $14.82 | +62.51 | — | +0.00 | +62.51 | +55.93 | — |
| 2026-08-26 | `TME` | 278 | — | $8.71 | +0.00 | $8.80 | +25.02 | +25.02 | +0.00 | +25.02 |
| 2026-08-26 | `SJM` | 17 | — | $134.80 | +0.00 | $130.90 | -66.30 | -66.30 | +0.00 | -66.30 |
| 2026-08-26 | `TAL` | 207 | — | $11.68 | +0.00 | $11.69 | +2.07 | +2.07 | +0.00 | +2.07 |
| 2026-08-26 | `NCNO` | 125 | — | $19.33 | +0.00 | $21.51 | +272.50 | +272.50 | +0.00 | +272.50 |
| 2026-08-27 | `TME` | 278 | $8.80 | $8.80 | +0.00 | — | +0.00 | +0.00 | +25.02 | — |
| 2026-08-27 | `SJM` | 17 | $130.90 | $130.29 | -10.37 | — | +0.00 | -10.37 | -76.67 | — |
| 2026-08-27 | `TAL` | 207 | $11.69 | $11.62 | -14.49 | — | +0.00 | -14.49 | -12.42 | — |
| 2026-08-27 | `NCNO` | 125 | $21.51 | $22.03 | +65.00 | — | +0.00 | +65.00 | +337.50 | — |
| 2026-08-28 | `PLAB` | 110 | — | $30.01 | +0.00 | $27.73 | -250.80 | -250.80 | +0.00 | -250.80 |
| 2026-08-28 | `WSM` | 14 | — | $235.67 | +0.00 | $235.09 | -8.12 | -8.12 | +0.00 | -8.12 |
| 2026-08-28 | `ULTA` | 6 | — | $542.00 | +0.00 | $517.50 | -147.00 | -147.00 | +0.00 | -147.00 |
| 2026-08-31 | `PLAB` | 110 | $27.73 | $28.04 | +34.10 | — | +0.00 | +34.10 | -216.70 | — |
| 2026-08-31 | `WSM` | 14 | $235.09 | $232.06 | -42.42 | — | +0.00 | -42.42 | -50.54 | — |
| 2026-08-31 | `ULTA` | 6 | $517.50 | $521.10 | +21.60 | — | +0.00 | +21.60 | -125.40 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `SNN` | 109 | — | $29.03 | +0.00 | $28.69 | -37.06 | -37.06 | +0.00 | -37.06 |
| 2026-09-03 | `MOMO` | 578 | — | $5.50 | +0.00 | $5.10 | -231.20 | -231.20 | +0.00 | -231.20 |
| 2026-09-03 | `VSXY` | 41 | — | $76.86 | +0.00 | $73.64 | -132.02 | -132.02 | +0.00 | -132.02 |
| 2026-09-04 | `SNN` | 109 | $28.69 | $28.77 | +8.72 | — | +0.00 | +8.72 | -28.34 | — |
| 2026-09-04 | `MOMO` | 578 | $5.10 | $5.13 | +17.34 | — | +0.00 | +17.34 | -213.86 | — |
| 2026-09-04 | `VSXY` | 41 | $73.64 | $73.63 | -0.41 | — | +0.00 | -0.41 | -132.43 | — |
| 2026-09-04 | `HAFN` | 1021 | — | $8.94 | +0.00 | $9.22 | +285.88 | +285.88 | +0.00 | +285.88 |
| 2026-09-08 | `HAFN` | 1021 | $9.22 | $8.81 | -418.61 | — | +0.00 | -418.61 | -132.73 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `GNW` | 897 | — | $10.00 | +0.00 | $10.09 | +80.73 | +80.73 | +0.00 | +80.73 |
| 2026-09-17 | `GNW` | 897 | $10.09 | $10.12 | +26.91 | — | +0.00 | +26.91 | +107.64 | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -34.46 | BSBR | — | $1.60 | $9,943.31 | BSBR×1723 |
| 2026-08-17 | +2.25 | $1.60 | BSBR×1723 | $9,960.54 | +17.23 | -38.75 | WBS | BSBR | $60.59 | $9,896.84 | WBS×125 |
| 2026-08-18 | -6.20 | $60.59 | WBS×125 | $9,875.59 | -21.25 | +0.00 | — | WBS | $9,873.12 | $9,873.12 | — |
| 2026-08-19 | -7.20 | $9,873.12 | — | $9,873.12 | +0.00 | +0.00 | — | — | $9,873.12 | $9,873.12 | — |
| 2026-08-20 | +1.12 | $9,873.12 | — | $9,873.12 | +0.00 | -408.36 | FUTU | — | $105.93 | $9,462.52 | FUTU×83 |
| 2026-08-21 | +3.25 | $105.93 | FUTU×83 | $9,665.87 | +203.35 | +0.00 | — | FUTU | $9,663.54 | $9,663.54 | — |
| 2026-08-24 | -5.17 | $9,663.54 | — | $9,663.54 | +0.00 | +0.00 | — | — | $9,663.54 | $9,663.54 | — |
| 2026-08-25 | +1.80 | $9,663.54 | — | $9,663.54 | +0.00 | -16.73 | AMX, SAN | — | $5.43 | $9,639.95 | AMX×203, SAN×329 |
| 2026-08-26 | +2.02 | $5.43 | AMX×203, SAN×329 | $9,702.46 | +62.51 | +233.29 | TME, SJM, TAL, NCNO | AMX, SAN | $137.78 | $9,918.06 | TME×278, SJM×17, TAL×207, NCNO×125 |
| 2026-08-27 | — | $137.78 | TME×278, SJM×17, TAL×207, NCNO×125 | $9,958.20 | +40.14 | +0.00 | — | TME, SJM, TAL, NCNO | $9,947.35 | $9,947.35 | — |
| 2026-08-28 | +0.75 | $9,947.35 | — | $9,947.35 | -0.00 | -405.92 | PLAB, WSM, ULTA | — | $88.51 | $9,535.07 | PLAB×110, WSM×14, ULTA×6 |
| 2026-08-31 | -5.85 | $88.51 | PLAB×110, WSM×14, ULTA×6 | $9,548.35 | +13.28 | +0.00 | — | PLAB, WSM, ULTA | $9,541.87 | $9,541.87 | — |
| 2026-09-01 | -6.30 | $9,541.87 | — | $9,541.87 | +0.00 | +0.00 | — | — | $9,541.87 | $9,541.87 | — |
| 2026-09-02 | -3.83 | $9,541.87 | — | $9,541.87 | +0.00 | +0.00 | — | — | $9,541.87 | $9,541.87 | — |
| 2026-09-03 | -0.90 | $9,541.87 | — | $9,541.87 | +0.00 | -400.28 | SNN, MOMO, VSXY | — | $35.46 | $9,129.71 | SNN×109, MOMO×578, VSXY×41 |
| 2026-09-04 | +2.25 | $35.46 | SNN×109, MOMO×578, VSXY×41 | $9,155.36 | +25.65 | +285.88 | HAFN | SNN, MOMO, VSXY | $2.36 | $9,415.98 | HAFN×1021 |
| 2026-09-08 | -11.47 | $2.36 | HAFN×1021 | $8,997.37 | -418.61 | +0.00 | — | HAFN | $8,983.96 | $8,983.96 | — |
| 2026-09-09 | -13.95 | $8,983.96 | — | $8,983.96 | +0.00 | +0.00 | — | — | $8,983.96 | $8,983.96 | — |
| 2026-09-10 | -13.28 | $8,983.96 | — | $8,983.96 | +0.00 | +0.00 | — | — | $8,983.96 | $8,983.96 | — |
| 2026-09-11 | +0.50 | $8,983.96 | — | $8,983.96 | +0.00 | +0.00 | — | — | $8,983.96 | $8,983.96 | — |
| 2026-09-14 | -11.00 | $8,983.96 | — | $8,983.96 | +0.00 | +0.00 | — | — | $8,983.96 | $8,983.96 | — |
| 2026-09-15 | -3.84 | $8,983.96 | — | $8,983.96 | +0.00 | +0.00 | — | — | $8,983.96 | $8,983.96 | — |
| 2026-09-16 | +5.30 | $8,983.96 | — | $8,983.96 | +0.00 | +80.73 | GNW | — | $2.39 | $9,053.12 | GNW×897 |
| 2026-09-17 | +7.38 | $2.39 | GNW×897 | $9,080.03 | +26.91 | +0.00 | — | GNW | $9,068.24 | $9,068.24 | — |
| 2026-09-18 | +4.86 | $9,068.24 | — | $9,068.24 | -0.00 | +0.00 | — | — | $9,068.24 | $9,068.24 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BSBR` | 1723 | $5.79 | $22.23 | — | $1.60 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list oppset; 🔵; ⚪; ret5=-0.3; leftover $10000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.60 | ▼ close $9,943.31 vs 09:30 $10,000.00 (session -34.46) | 16:00 close · cash $1.60 · equity $9,943.31 vs 09:30 $10,000.00 (-56.69; session marks -34.46) · 1 name(s) marked open→close (per-name table). BSBR×1723 09:30 $5.79 → close $5.77 -34.46 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.60 | ▲ 09:30 equity $9,960.54 vs yday $9,943.31 (+17.23) | 09:30 open · cash $1.60 (unchanged overnight, no fees) · equity $9,960.54 vs prior close $9,943.31 (+17.23) · 1 name(s) re-marked at the open (per-name table). BSBR×1723 yday $5.77 → 09:30 $5.78 +17.23 | — |
| 2026-08-17 09:30 ET | **SELL** | `BSBR` | 1723 | $5.78 | $22.59 | $-62.05 | $9,937.95 | ▼ -62.05 after sell → book $9,937.95; vs 09:30 mark -22.59 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `WBS` | 125 | $79.00 | $2.37 | — | $60.59 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list oppset; ⚪; ret5=+0.5; leftover $9937.95 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $60.59 | ▼ close $9,896.84 vs 09:30 $9,960.54 (session -38.75) | 16:00 close · cash $60.59 · equity $9,896.84 vs 09:30 $9,960.54 (-63.70; session marks -38.75) · 1 name(s) marked open→close (per-name table). WBS×125 09:30 $79.00 → close $78.69 -38.75 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.59 | ▼ 09:30 equity $9,875.59 vs yday $9,896.84 (-21.25) | 09:30 open · cash $60.59 (unchanged overnight, no fees) · equity $9,875.59 vs prior close $9,896.84 (-21.25) · 1 name(s) re-marked at the open (per-name table). WBS×125 yday $78.69 → 09:30 $78.52 -21.25 | — |
| 2026-08-18 09:30 ET | **SELL** | `WBS` | 125 | $78.52 | $2.46 | $-64.83 | $9,873.12 | ▼ -64.83 after sell → book $9,873.12; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,873.12 | ▲ close $9,873.12 vs 09:30 $9,875.59 (session +0.00) | 16:00 close · cash $9,873.12 · no lots left · equity $9,873.12. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,873.12 | ▲ 09:30 equity $9,873.12 vs yday $9,873.12 (+0.00) | 09:30 open · cash $9,873.12 · no holdings · equity $9,873.12 vs prior close $9,873.12 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,873.12 | ▲ close $9,873.12 vs 09:30 $9,873.12 (session +0.00) | 16:00 close · cash $9,873.12 · no lots left · equity $9,873.12. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,873.12 | ▲ 09:30 equity $9,873.12 vs yday $9,873.12 (+0.00) | 09:30 open · cash $9,873.12 · no holdings · equity $9,873.12 vs prior close $9,873.12 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 83 | $117.65 | $2.24 | — | $105.93 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; 🔵; ret5=+4.1; leftover $9873.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $105.93 | ▼ close $9,462.52 vs 09:30 $9,873.12 (session -408.36) | 16:00 close · cash $105.93 · equity $9,462.52 vs 09:30 $9,873.12 (-410.60; session marks -408.36) · 1 name(s) marked open→close (per-name table). FUTU×83 09:30 $117.65 → close $112.73 -408.36 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $105.93 | ▲ 09:30 equity $9,665.87 vs yday $9,462.52 (+203.35) | 09:30 open · cash $105.93 (unchanged overnight, no fees) · equity $9,665.87 vs prior close $9,462.52 (+203.35) · 1 name(s) re-marked at the open (per-name table). FUTU×83 yday $112.73 → 09:30 $115.18 +203.35 | — |
| 2026-08-21 09:30 ET | **SELL** | `FUTU` | 83 | $115.18 | $2.33 | $-209.58 | $9,663.54 | ▼ -209.58 after sell → book $9,663.54; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,663.54 | ▲ close $9,663.54 vs 09:30 $9,665.87 (session +0.00) | 16:00 close · cash $9,663.54 · no lots left · equity $9,663.54. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,663.54 | ▲ 09:30 equity $9,663.54 vs yday $9,663.54 (+0.00) | 09:30 open · cash $9,663.54 · no holdings · equity $9,663.54 vs prior close $9,663.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,663.54 | ▲ close $9,663.54 vs 09:30 $9,663.54 (session +0.00) | 16:00 close · cash $9,663.54 · no lots left · equity $9,663.54. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,663.54 | ▲ 09:30 equity $9,663.54 vs yday $9,663.54 (+0.00) | 09:30 open · cash $9,663.54 · no holdings · equity $9,663.54 vs prior close $9,663.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `AMX` | 203 | $23.80 | $2.62 | — | $4,829.52 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list oppset; 🔵; ret5=+0.5; leftover $4831.77 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAN` | 329 | $14.65 | $4.24 | — | $5.43 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list oppset; 🔵; ret5=+0.9; leftover $4831.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.43 | ▼ close $9,639.95 vs 09:30 $9,663.54 (session -16.73) | 16:00 close · cash $5.43 · equity $9,639.95 vs 09:30 $9,663.54 (-23.59; session marks -16.73) · 2 name(s) marked open→close (per-name table). AMX×203 09:30 $23.80 → close $23.75 -10.15; SAN×329 09:30 $14.65 → close $14.63 -6.58 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.43 | ▲ 09:30 equity $9,702.46 vs yday $9,639.95 (+62.51) | 09:30 open · cash $5.43 (unchanged overnight, no fees) · equity $9,702.46 vs prior close $9,639.95 (+62.51) · 2 name(s) re-marked at the open (per-name table). AMX×203 yday $23.75 → 09:30 $23.75 +0.00; SAN×329 yday $14.63 → 09:30 $14.82 +62.51 | — |
| 2026-08-26 09:30 ET | **SELL** | `AMX` | 203 | $23.75 | $2.69 | $-15.46 | $4,823.99 | ▼ -15.46 after sell → book $9,699.77; vs 09:30 mark -2.69 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `SAN` | 329 | $14.82 | $4.34 | $+47.35 | $9,695.43 | ▲ +47.35 after sell → book $9,695.43; vs 09:30 mark -4.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `TME` | 278 | $8.71 | $3.59 | — | $7,270.47 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list oppset; 🔵; ret5=-0.6; leftover $2423.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SJM` | 17 | $134.80 | $2.04 | — | $4,976.82 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; 🔵; ret5=+5.9; leftover $2423.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TAL` | 207 | $11.68 | $2.67 | — | $2,556.39 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list oppset; 🔵; ret5=-3.7; leftover $2423.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 125 | $19.33 | $2.37 | — | $137.78 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; ret5=+3.0; leftover $2423.86 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.78 | ▲ close $9,918.06 vs 09:30 $9,702.46 (session +233.29) | 16:00 close · cash $137.78 · equity $9,918.06 vs 09:30 $9,702.46 (+215.60; session marks +233.29) · 4 name(s) marked open→close (per-name table). TME×278 09:30 $8.71 → close $8.80 +25.02; SJM×17 09:30 $134.80 → close $130.90 -66.30; TAL×207 09:30 $11.68 → close $11.69 +2.07; NCNO×125 09:30 $19.33 → close $21.51 +272.50 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.78 | ▲ 09:30 equity $9,958.20 vs yday $9,918.06 (+40.14) | 09:30 open · cash $137.78 (unchanged overnight, no fees) · equity $9,958.20 vs prior close $9,918.06 (+40.14) · 4 name(s) re-marked at the open (per-name table). TME×278 yday $8.80 → 09:30 $8.80 +0.00; SJM×17 yday $130.90 → 09:30 $130.29 -10.37; TAL×207 yday $11.69 → 09:30 $11.62 -14.49; NCNO×125 yday $21.51 → 09:30 $22.03 +65.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `TME` | 278 | $8.80 | $3.65 | $+17.78 | $2,580.53 | ▲ +17.78 after sell → book $9,954.55; vs 09:30 mark -3.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SJM` | 17 | $130.29 | $2.07 | $-80.78 | $4,793.39 | ▼ -80.78 after sell → book $9,952.48; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `TAL` | 207 | $11.62 | $2.72 | $-17.81 | $7,196.00 | ▼ -17.81 after sell → book $9,949.75; vs 09:30 mark -2.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 125 | $22.03 | $2.41 | $+332.73 | $9,947.35 | ▲ +332.73 after sell → book $9,947.35; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,947.35 | ▲ close $9,947.35 vs 09:30 $9,958.20 (session +0.00) | 16:00 close · cash $9,947.35 · no lots left · equity $9,947.35. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,947.35 | ▲ 09:30 equity $9,947.35 vs yday $9,947.35 (-0.00) | 09:30 open · cash $9,947.35 · no holdings · equity $9,947.35 vs prior close $9,947.35 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `PLAB` | 110 | $30.01 | $2.32 | — | $6,643.93 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list oppset; 🔵; ret5=-0.9; leftover $3315.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `WSM` | 14 | $235.67 | $2.03 | — | $3,342.52 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list oppset; ret5=+1.1; leftover $3315.78 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 6 | $542.00 | $2.01 | — | $88.51 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; ret5=+4.8; leftover $3315.78 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.51 | ▼ close $9,535.07 vs 09:30 $9,947.35 (session -405.92) | 16:00 close · cash $88.51 · equity $9,535.07 vs 09:30 $9,947.35 (-412.28; session marks -405.92) · 3 name(s) marked open→close (per-name table). PLAB×110 09:30 $30.01 → close $27.73 -250.80; WSM×14 09:30 $235.67 → close $235.09 -8.12; ULTA×6 09:30 $542.00 → close $517.50 -147.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.51 | ▲ 09:30 equity $9,548.35 vs yday $9,535.07 (+13.28) | 09:30 open · cash $88.51 (unchanged overnight, no fees) · equity $9,548.35 vs prior close $9,535.07 (+13.28) · 3 name(s) re-marked at the open (per-name table). PLAB×110 yday $27.73 → 09:30 $28.04 +34.10; WSM×14 yday $235.09 → 09:30 $232.06 -42.42; ULTA×6 yday $517.50 → 09:30 $521.10 +21.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `PLAB` | 110 | $28.04 | $2.36 | $-221.38 | $3,170.54 | ▼ -221.38 after sell → book $9,545.98; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `WSM` | 14 | $232.06 | $2.07 | $-54.64 | $6,417.32 | ▼ -54.64 after sell → book $9,543.92; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ULTA` | 6 | $521.10 | $2.04 | $-129.45 | $9,541.87 | ▼ -129.45 after sell → book $9,541.87; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,541.87 | ▲ close $9,541.87 vs 09:30 $9,548.35 (session +0.00) | 16:00 close · cash $9,541.87 · no lots left · equity $9,541.87. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,541.87 | ▲ 09:30 equity $9,541.87 vs yday $9,541.87 (+0.00) | 09:30 open · cash $9,541.87 · no holdings · equity $9,541.87 vs prior close $9,541.87 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,541.87 | ▲ close $9,541.87 vs 09:30 $9,541.87 (session +0.00) | 16:00 close · cash $9,541.87 · no lots left · equity $9,541.87. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,541.87 | ▲ 09:30 equity $9,541.87 vs yday $9,541.87 (+0.00) | 09:30 open · cash $9,541.87 · no holdings · equity $9,541.87 vs prior close $9,541.87 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,541.87 | ▲ close $9,541.87 vs 09:30 $9,541.87 (session +0.00) | 16:00 close · cash $9,541.87 · no lots left · equity $9,541.87. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,541.87 | ▲ 09:30 equity $9,541.87 vs yday $9,541.87 (+0.00) | 09:30 open · cash $9,541.87 · no holdings · equity $9,541.87 vs prior close $9,541.87 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `SNN` | 109 | $29.03 | $2.32 | — | $6,375.29 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list oppset; 🔵; ret5=-2.2; leftover $3180.62 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 578 | $5.50 | $7.46 | — | $3,188.83 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; 🔵; ret5=-4.8; leftover $3180.62 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 41 | $76.86 | $2.11 | — | $35.46 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list earn_react; 🔵; ret5=-6.6; leftover $3180.62 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.46 | ▼ close $9,129.71 vs 09:30 $9,541.87 (session -400.28) | 16:00 close · cash $35.46 · equity $9,129.71 vs 09:30 $9,541.87 (-412.16; session marks -400.28) · 3 name(s) marked open→close (per-name table). SNN×109 09:30 $29.03 → close $28.69 -37.06; MOMO×578 09:30 $5.50 → close $5.10 -231.20; VSXY×41 09:30 $76.86 → close $73.64 -132.02 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.46 | ▲ 09:30 equity $9,155.36 vs yday $9,129.71 (+25.65) | 09:30 open · cash $35.46 (unchanged overnight, no fees) · equity $9,155.36 vs prior close $9,129.71 (+25.65) · 3 name(s) re-marked at the open (per-name table). SNN×109 yday $28.69 → 09:30 $28.77 +8.72; MOMO×578 yday $5.10 → 09:30 $5.13 +17.34; VSXY×41 yday $73.64 → 09:30 $73.63 -0.41 | — |
| 2026-09-04 09:30 ET | **SELL** | `SNN` | 109 | $28.77 | $2.36 | $-33.02 | $3,169.03 | ▼ -33.02 after sell → book $9,153.00; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MOMO` | 578 | $5.13 | $7.58 | $-228.89 | $6,126.59 | ▼ -228.89 after sell → book $9,145.42; vs 09:30 mark -7.58 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 41 | $73.63 | $2.15 | $-136.69 | $9,143.27 | ▼ -136.69 after sell → book $9,143.27; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HAFN` | 1021 | $8.94 | $13.17 | — | $2.36 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list ohlc_hot; ret5=+7.7; leftover $9143.27 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.36 | ▲ close $9,415.98 vs 09:30 $9,155.36 (session +285.88) | 16:00 close · cash $2.36 · equity $9,415.98 vs 09:30 $9,155.36 (+260.62; session marks +285.88) · 1 name(s) marked open→close (per-name table). HAFN×1021 09:30 $8.94 → close $9.22 +285.88 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.36 | ▼ 09:30 equity $8,997.37 vs yday $9,415.98 (-418.61) | 09:30 open · cash $2.36 (unchanged overnight, no fees) · equity $8,997.37 vs prior close $9,415.98 (-418.61) · 1 name(s) re-marked at the open (per-name table). HAFN×1021 yday $9.22 → 09:30 $8.81 -418.61 | — |
| 2026-09-08 09:30 ET | **SELL** | `HAFN` | 1021 | $8.81 | $13.41 | $-159.31 | $8,983.96 | ▼ -159.31 after sell → book $8,983.96; vs 09:30 mark -13.41 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,983.96 | ▲ close $8,983.96 vs 09:30 $8,997.37 (session +0.00) | 16:00 close · cash $8,983.96 · no lots left · equity $8,983.96. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,983.96 | ▲ 09:30 equity $8,983.96 vs yday $8,983.96 (+0.00) | 09:30 open · cash $8,983.96 · no holdings · equity $8,983.96 vs prior close $8,983.96 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,983.96 | ▲ close $8,983.96 vs 09:30 $8,983.96 (session +0.00) | 16:00 close · cash $8,983.96 · no lots left · equity $8,983.96. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,983.96 | ▲ 09:30 equity $8,983.96 vs yday $8,983.96 (+0.00) | 09:30 open · cash $8,983.96 · no holdings · equity $8,983.96 vs prior close $8,983.96 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,983.96 | ▲ close $8,983.96 vs 09:30 $8,983.96 (session +0.00) | 16:00 close · cash $8,983.96 · no lots left · equity $8,983.96. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,983.96 | ▲ 09:30 equity $8,983.96 vs yday $8,983.96 (+0.00) | 09:30 open · cash $8,983.96 · no holdings · equity $8,983.96 vs prior close $8,983.96 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,983.96 | ▲ close $8,983.96 vs 09:30 $8,983.96 (session +0.00) | 16:00 close · cash $8,983.96 · no lots left · equity $8,983.96. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,983.96 | ▲ 09:30 equity $8,983.96 vs yday $8,983.96 (+0.00) | 09:30 open · cash $8,983.96 · no holdings · equity $8,983.96 vs prior close $8,983.96 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,983.96 | ▲ close $8,983.96 vs 09:30 $8,983.96 (session +0.00) | 16:00 close · cash $8,983.96 · no lots left · equity $8,983.96. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,983.96 | ▲ 09:30 equity $8,983.96 vs yday $8,983.96 (+0.00) | 09:30 open · cash $8,983.96 · no holdings · equity $8,983.96 vs prior close $8,983.96 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,983.96 | ▲ close $8,983.96 vs 09:30 $8,983.96 (session +0.00) | 16:00 close · cash $8,983.96 · no lots left · equity $8,983.96. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,983.96 | ▲ 09:30 equity $8,983.96 vs yday $8,983.96 (+0.00) | 09:30 open · cash $8,983.96 · no holdings · equity $8,983.96 vs prior close $8,983.96 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `GNW` | 897 | $10.00 | $11.57 | — | $2.39 | — | Clock-B #8 flow-in + green + not extended; gate clk_flow_coil=True; rank cond; list oppset; 🔵; ret5=+4.8; leftover $8983.96 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.39 | ▲ close $9,053.12 vs 09:30 $8,983.96 (session +80.73) | 16:00 close · cash $2.39 · equity $9,053.12 vs 09:30 $8,983.96 (+69.16; session marks +80.73) · 1 name(s) marked open→close (per-name table). GNW×897 09:30 $10.00 → close $10.09 +80.73 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.39 | ▲ 09:30 equity $9,080.03 vs yday $9,053.12 (+26.91) | 09:30 open · cash $2.39 (unchanged overnight, no fees) · equity $9,080.03 vs prior close $9,053.12 (+26.91) · 1 name(s) re-marked at the open (per-name table). GNW×897 yday $10.09 → 09:30 $10.12 +26.91 | — |
| 2026-09-17 09:30 ET | **SELL** | `GNW` | 897 | $10.12 | $11.79 | $+84.28 | $9,068.24 | ▲ +84.28 after sell → book $9,068.24; vs 09:30 mark -11.79 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,068.24 | ▲ close $9,068.24 vs 09:30 $9,080.03 (session +0.00) | 16:00 close · cash $9,068.24 · no lots left · equity $9,068.24. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,068.24 | ▲ 09:30 equity $9,068.24 vs yday $9,068.24 (-0.00) | 09:30 open · cash $9,068.24 · no holdings · equity $9,068.24 vs prior close $9,068.24 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,068.24 | ▲ close $9,068.24 vs 09:30 $9,068.24 (session +0.00) | 16:00 close · cash $9,068.24 · no lots left · equity $9,068.24. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `EIX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
