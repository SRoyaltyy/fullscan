# Factor mine action — `union_flow_in_white_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-5.14%** ($9,486) · signal-only (no cash/fees) was -2.17%. Starts YES **0/26**. Fills 13 · skips 29 · realized $-247.53.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: money came in (prior rel vol ≥ 1.5) but price barely moved (|1-day| ≤ 1.2%).
- Must-have: no morning camera is red (the 'white' / all-clear row).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

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
- **Gate** `flow_in=True,zero_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9.31.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TPG` | 197 | — | $50.62 | +0.00 | $54.62 | +787.37 | +787.37 | +0.00 | +787.37 |
| 2026-08-14 | `TPG` | 197 | $54.62 | $55.29 | +131.99 | $53.03 | -445.22 | -313.23 | +919.36 | +474.14 |
| 2026-08-14 | `KULR` | 1 | — | $2.50 | +0.00 | $2.64 | +0.14 | +0.14 | +0.00 | +0.14 |
| 2026-08-14 | `RLX` | 1 | — | $1.85 | +0.00 | $1.94 | +0.09 | +0.09 | +0.00 | +0.09 |
| 2026-08-17 | `TPG` | 197 | $53.03 | $52.67 | -70.92 | $51.77 | -177.30 | -248.22 | +403.22 | +225.92 |
| 2026-08-17 | `KULR` | 1 | $2.64 | $2.63 | -0.01 | $2.62 | -0.01 | -0.02 | +0.13 | +0.12 |
| 2026-08-17 | `RLX` | 1 | $1.94 | $1.92 | -0.02 | $1.87 | -0.05 | -0.07 | +0.07 | +0.02 |
| 2026-08-18 | `TPG` | 197 | $51.77 | $51.77 | +0.00 | — | +0.00 | +0.00 | +225.92 | — |
| 2026-08-18 | `KULR` | 1 | $2.62 | $2.53 | -0.09 | $2.57 | +0.04 | -0.05 | +0.03 | +0.07 |
| 2026-08-18 | `RLX` | 1 | $1.87 | $1.86 | -0.01 | $1.83 | -0.03 | -0.04 | +0.01 | -0.02 |
| 2026-08-19 | `KULR` | 1 | $2.57 | $2.55 | -0.02 | — | +0.00 | -0.02 | +0.05 | — |
| 2026-08-19 | `RLX` | 1 | $1.83 | $1.84 | +0.01 | — | +0.00 | +0.01 | -0.01 | — |
| 2026-08-20 | `WBS` | 131 | — | $77.57 | +0.00 | $77.57 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-21 | `WBS` | 131 | $77.57 | $77.57 | +0.00 | $77.57 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-21 | `HITI` | 22 | — | $2.43 | +0.00 | $2.45 | +0.44 | +0.44 | +0.00 | +0.44 |
| 2026-08-24 | `WBS` | 131 | $77.57 | $77.57 | +0.00 | $77.57 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-24 | `HITI` | 22 | $2.45 | $2.45 | +0.00 | $2.46 | +0.22 | +0.22 | +0.44 | +0.66 |
| 2026-08-25 | `WBS` | 131 | $77.57 | $77.57 | +0.00 | $77.57 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `HITI` | 22 | $2.46 | $2.48 | +0.44 | $2.57 | +1.98 | +2.42 | +1.10 | +3.08 |
| 2026-08-26 | `WBS` | 131 | $77.57 | $77.57 | +0.00 | $77.57 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `HITI` | 22 | $2.57 | $2.57 | +0.00 | — | +0.00 | +0.00 | +3.08 | — |
| 2026-08-27 | `WBS` | 131 | $77.57 | $77.57 | +0.00 | $77.57 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-28 | `WBS` | 131 | $77.57 | $77.57 | +0.00 | — | +0.00 | +0.00 | +0.00 | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `VIR` | 884 | — | $11.54 | +0.00 | $11.45 | -79.56 | -79.56 | +0.00 | -79.56 |
| 2026-09-04 | `VIR` | 884 | $11.45 | $11.31 | -123.76 | $11.38 | +66.30 | -57.46 | -203.32 | -137.02 |
| 2026-09-08 | `VIR` | 884 | $11.38 | $11.22 | -145.86 | $11.18 | -35.36 | -181.22 | -282.88 | -318.24 |
| 2026-09-09 | `VIR` | 884 | $11.18 | $11.04 | -123.76 | — | +0.00 | -123.76 | -442.00 | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | `S` | 421 | — | $23.13 | +0.00 | $22.51 | -261.02 | -261.02 | +0.00 | -261.02 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +787.37 | TPG | — | $24.65 | $10,784.79 | TPG×197 |
| 2026-08-14 | +5.50 | $24.65 | TPG×197 | $10,916.78 | +131.99 | -444.99 | KULR, RLX | — | $20.25 | $10,471.74 | TPG×197, KULR×1, RLX×1 |
| 2026-08-17 | +2.25 | $20.25 | TPG×197, KULR×1, RLX×1 | $10,400.79 | -70.95 | -177.36 | — | — | $20.25 | $10,223.43 | TPG×197, KULR×1, RLX×1 |
| 2026-08-18 | -6.20 | $20.25 | TPG×197, KULR×1, RLX×1 | $10,223.33 | -0.10 | +0.01 | — | TPG | $10,216.24 | $10,220.64 | KULR×1, RLX×1 |
| 2026-08-19 | -7.20 | $10,216.24 | KULR×1, RLX×1 | $10,220.63 | -0.01 | +0.00 | — | KULR, RLX | $10,220.54 | $10,220.54 | — |
| 2026-08-20 | +1.12 | $10,220.54 | — | $10,220.54 | +0.00 | +0.00 | WBS | — | $56.49 | $10,218.16 | WBS×131 |
| 2026-08-21 | +3.25 | $56.49 | WBS×131 | $10,218.16 | +0.00 | +0.44 | HITI | — | $2.43 | $10,218.00 | WBS×131, HITI×22 |
| 2026-08-24 | -5.17 | $2.43 | WBS×131, HITI×22 | $10,218.00 | +0.00 | +0.22 | — | — | $2.43 | $10,218.22 | WBS×131, HITI×22 |
| 2026-08-25 | +1.80 | $2.43 | WBS×131, HITI×22 | $10,218.66 | +0.44 | +1.98 | — | — | $2.43 | $10,220.64 | WBS×131, HITI×22 |
| 2026-08-26 | +2.02 | $2.43 | WBS×131, HITI×22 | $10,220.64 | +0.00 | +0.00 | — | HITI | $58.32 | $10,219.99 | WBS×131 |
| 2026-08-27 | — | $58.32 | WBS×131 | $10,219.99 | -0.00 | +0.00 | — | — | $58.32 | $10,219.99 | WBS×131 |
| 2026-08-28 | +0.75 | $58.32 | WBS×131 | $10,219.99 | -0.00 | +0.00 | — | WBS | $10,217.50 | $10,217.50 | — |
| 2026-08-31 | -5.85 | $10,217.50 | — | $10,217.50 | +0.00 | +0.00 | — | — | $10,217.50 | $10,217.50 | — |
| 2026-09-01 | -6.30 | $10,217.50 | — | $10,217.50 | +0.00 | +0.00 | — | — | $10,217.50 | $10,217.50 | — |
| 2026-09-02 | -3.83 | $10,217.50 | — | $10,217.50 | +0.00 | +0.00 | — | — | $10,217.50 | $10,217.50 | — |
| 2026-09-03 | -0.90 | $10,217.50 | — | $10,217.50 | +0.00 | -79.56 | VIR | — | $4.74 | $10,126.54 | VIR×884 |
| 2026-09-04 | +2.25 | $4.74 | VIR×884 | $10,002.78 | -123.76 | +66.30 | — | — | $4.74 | $10,069.08 | VIR×884 |
| 2026-09-08 | -11.47 | $4.74 | VIR×884 | $9,923.22 | -145.86 | -35.36 | — | — | $4.74 | $9,887.86 | VIR×884 |
| 2026-09-09 | -13.95 | $4.74 | VIR×884 | $9,764.10 | -123.76 | +0.00 | — | VIR | $9,752.47 | $9,752.47 | — |
| 2026-09-10 | -13.28 | $9,752.47 | — | $9,752.47 | +0.00 | +0.00 | — | — | $9,752.47 | $9,752.47 | — |
| 2026-09-11 | +0.50 | $9,752.47 | — | $9,752.47 | +0.00 | +0.00 | — | — | $9,752.47 | $9,752.47 | — |
| 2026-09-14 | -11.00 | $9,752.47 | — | $9,752.47 | +0.00 | +0.00 | — | — | $9,752.47 | $9,752.47 | — |
| 2026-09-15 | -3.84 | $9,752.47 | — | $9,752.47 | +0.00 | +0.00 | — | — | $9,752.47 | $9,752.47 | — |
| 2026-09-16 | +5.30 | $9,752.47 | — | $9,752.47 | +0.00 | +0.00 | — | — | $9,752.47 | $9,752.47 | — |
| 2026-09-17 | +7.38 | $9,752.47 | — | $9,752.47 | +0.00 | +0.00 | — | — | $9,752.47 | $9,752.47 | — |
| 2026-09-18 | +4.86 | $9,752.47 | — | $9,752.47 | +0.00 | -261.02 | S | — | $9.31 | $9,486.02 | S×421 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | combo gate; gate flow_in=True,zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | 16:00 close · cash $24.65 · equity $10,784.79 vs 09:30 $10,000.00 (+784.79; session marks +787.37) · 1 name(s) marked open→close (per-name table). TPG×197 09:30 $50.62 → close $54.62 +787.37 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | 09:30 open · cash $24.65 (unchanged overnight, no fees) · equity $10,916.78 vs prior close $10,784.79 (+131.99) · 1 name(s) re-marked at the open (per-name table). TPG×197 yday $54.62 → 09:30 $55.29 +131.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 1 | $2.50 | $0.03 | — | $22.12 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $3.52 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `RLX` | 1 | $1.85 | $0.02 | — | $20.25 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+0.5; leftover $3.52 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.25 | ▼ close $10,471.74 vs 09:30 $10,916.78 (session -444.99) | 16:00 close · cash $20.25 · equity $10,471.74 vs 09:30 $10,916.78 (-445.04; session marks -444.99) · 3 name(s) marked open→close (per-name table). TPG×197 09:30 $55.29 → close $53.03 -445.22; KULR×1 09:30 $2.50 → close $2.64 +0.14; RLX×1 09:30 $1.85 → close $1.94 +0.09 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.25 | ▼ 09:30 equity $10,400.79 vs yday $10,471.74 (-70.95) | 09:30 open · cash $20.25 (unchanged overnight, no fees) · equity $10,400.79 vs prior close $10,471.74 (-70.95) · 3 name(s) re-marked at the open (per-name table). TPG×197 yday $53.03 → 09:30 $52.67 -70.92; KULR×1 yday $2.64 → 09:30 $2.63 -0.01; RLX×1 yday $1.94 → 09:30 $1.92 -0.02 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.25 | ▼ close $10,223.43 vs 09:30 $10,400.79 (session -177.36) | 16:00 close · cash $20.25 · equity $10,223.43 vs 09:30 $10,400.79 (-177.36; session marks -177.36) · 3 name(s) marked open→close (per-name table). TPG×197 09:30 $52.67 → close $51.77 -177.30; KULR×1 09:30 $2.63 → close $2.62 -0.01; RLX×1 09:30 $1.92 → close $1.87 -0.05 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.25 | ▼ 09:30 equity $10,223.33 vs yday $10,223.43 (-0.10) | 09:30 open · cash $20.25 (unchanged overnight, no fees) · equity $10,223.33 vs prior close $10,223.43 (-0.10) · 3 name(s) re-marked at the open (per-name table). TPG×197 yday $51.77 → 09:30 $51.77 +0.00; KULR×1 yday $2.62 → 09:30 $2.53 -0.09; RLX×1 yday $1.87 → 09:30 $1.86 -0.01 | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 197 | $51.77 | $2.70 | $+220.64 | $10,216.24 | ▲ +220.64 after sell → book $10,220.63; vs 09:30 mark -2.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,216.24 | ▲ close $10,220.64 vs 09:30 $10,223.33 (session +0.01) | 16:00 close · cash $10,216.24 · equity $10,220.64 vs 09:30 $10,223.33 (-2.69; session marks +0.01) · 2 name(s) marked open→close (per-name table). KULR×1 09:30 $2.53 → close $2.57 +0.04; RLX×1 09:30 $1.86 → close $1.83 -0.03 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,216.24 | ▼ 09:30 equity $10,220.63 vs yday $10,220.64 (-0.01) | 09:30 open · cash $10,216.24 (unchanged overnight, no fees) · equity $10,220.63 vs prior close $10,220.64 (-0.01) · 2 name(s) re-marked at the open (per-name table). KULR×1 yday $2.57 → 09:30 $2.55 -0.02; RLX×1 yday $1.83 → 09:30 $1.84 +0.01 | — |
| 2026-08-19 09:30 ET | **SELL** | `KULR` | 1 | $2.55 | $0.05 | $-0.03 | $10,218.75 | ▼ -0.03 after sell → book $10,220.59; vs 09:30 mark -0.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `RLX` | 1 | $1.84 | $0.04 | $-0.07 | $10,220.54 | ▼ -0.07 after sell → book $10,220.54; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,220.54 | ▲ close $10,220.54 vs 09:30 $10,220.63 (session +0.00) | 16:00 close · cash $10,220.54 · no lots left · equity $10,220.54. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,220.54 | ▲ 09:30 equity $10,220.54 vs yday $10,220.54 (+0.00) | 09:30 open · cash $10,220.54 · no holdings · equity $10,220.54 vs prior close $10,220.54 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `WBS` | 131 | $77.57 | $2.38 | — | $56.49 | — | combo gate; gate flow_in=True,zero_red=True; list oppset; 🔵; ⚪; ret5=-1.9; leftover $10220.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.49 | ▲ close $10,218.16 vs 09:30 $10,220.54 (session +0.00) | 16:00 close · cash $56.49 · equity $10,218.16 vs 09:30 $10,220.54 (-2.38; session marks +0.00) · 1 name(s) marked open→close (per-name table). WBS×131 09:30 $77.57 → close $77.57 +0.00 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.49 | ▲ 09:30 equity $10,218.16 vs yday $10,218.16 (+0.00) | 09:30 open · cash $56.49 (unchanged overnight, no fees) · equity $10,218.16 vs prior close $10,218.16 (+0.00) · 1 name(s) re-marked at the open (per-name table). WBS×131 yday $77.57 → 09:30 $77.57 +0.00 | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 22 | $2.43 | $0.60 | — | $2.43 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $56.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.43 | ▲ close $10,218.00 vs 09:30 $10,218.16 (session +0.44) | 16:00 close · cash $2.43 · equity $10,218.00 vs 09:30 $10,218.16 (-0.16; session marks +0.44) · 2 name(s) marked open→close (per-name table). WBS×131 09:30 $77.57 → close $77.57 +0.00; HITI×22 09:30 $2.43 → close $2.45 +0.44 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.43 | ▲ 09:30 equity $10,218.00 vs yday $10,218.00 (+0.00) | 09:30 open · cash $2.43 (unchanged overnight, no fees) · equity $10,218.00 vs prior close $10,218.00 (+0.00) · 2 name(s) re-marked at the open (per-name table). WBS×131 yday $77.57 → 09:30 $77.57 +0.00; HITI×22 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.43 | ▲ close $10,218.22 vs 09:30 $10,218.00 (session +0.22) | 16:00 close · cash $2.43 · equity $10,218.22 vs 09:30 $10,218.00 (+0.22; session marks +0.22) · 2 name(s) marked open→close (per-name table). WBS×131 09:30 $77.57 → close $77.57 +0.00; HITI×22 09:30 $2.45 → close $2.46 +0.22 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.43 | ▲ 09:30 equity $10,218.66 vs yday $10,218.22 (+0.44) | 09:30 open · cash $2.43 (unchanged overnight, no fees) · equity $10,218.66 vs prior close $10,218.22 (+0.44) · 2 name(s) re-marked at the open (per-name table). WBS×131 yday $77.57 → 09:30 $77.57 +0.00; HITI×22 yday $2.46 → 09:30 $2.48 +0.44 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.43 | ▲ close $10,220.64 vs 09:30 $10,218.66 (session +1.98) | 16:00 close · cash $2.43 · equity $10,220.64 vs 09:30 $10,218.66 (+1.98; session marks +1.98) · 2 name(s) marked open→close (per-name table). WBS×131 09:30 $77.57 → close $77.57 +0.00; HITI×22 09:30 $2.48 → close $2.57 +1.98 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.43 | ▲ 09:30 equity $10,220.64 vs yday $10,220.64 (+0.00) | 09:30 open · cash $2.43 (unchanged overnight, no fees) · equity $10,220.64 vs prior close $10,220.64 (+0.00) · 2 name(s) re-marked at the open (per-name table). WBS×131 yday $77.57 → 09:30 $77.57 +0.00; HITI×22 yday $2.57 → 09:30 $2.57 +0.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `HITI` | 22 | $2.57 | $0.65 | $+1.83 | $58.32 | ▲ +1.83 after sell → book $10,219.99; vs 09:30 mark -0.65 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.32 | ▲ close $10,219.99 vs 09:30 $10,220.64 (session +0.00) | 16:00 close · cash $58.32 · equity $10,219.99 vs 09:30 $10,220.64 (-0.65; session marks +0.00) · 1 name(s) marked open→close (per-name table). WBS×131 09:30 $77.57 → close $77.57 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $58.32 | ▲ 09:30 equity $10,219.99 vs yday $10,219.99 (-0.00) | 09:30 open · cash $58.32 (unchanged overnight, no fees) · equity $10,219.99 vs prior close $10,219.99 (-0.00) · 1 name(s) re-marked at the open (per-name table). WBS×131 yday $77.57 → 09:30 $77.57 +0.00 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.32 | ▲ close $10,219.99 vs 09:30 $10,219.99 (session +0.00) | 16:00 close · cash $58.32 · equity $10,219.99 vs 09:30 $10,219.99 (-0.00; session marks +0.00) · 1 name(s) marked open→close (per-name table). WBS×131 09:30 $77.57 → close $77.57 +0.00 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $58.32 | ▲ 09:30 equity $10,219.99 vs yday $10,219.99 (-0.00) | 09:30 open · cash $58.32 (unchanged overnight, no fees) · equity $10,219.99 vs prior close $10,219.99 (-0.00) · 1 name(s) re-marked at the open (per-name table). WBS×131 yday $77.57 → 09:30 $77.57 +0.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `WBS` | 131 | $77.57 | $2.49 | $-4.87 | $10,217.50 | ▼ -4.87 after sell → book $10,217.50; vs 09:30 mark -2.49 | dropped from list after 6 sess (min 3) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,217.50 | ▲ close $10,217.50 vs 09:30 $10,219.99 (session +0.00) | 16:00 close · cash $10,217.50 · no lots left · equity $10,217.50. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.50 | ▲ 09:30 equity $10,217.50 vs yday $10,217.50 (+0.00) | 09:30 open · cash $10,217.50 · no holdings · equity $10,217.50 vs prior close $10,217.50 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,217.50 | ▲ close $10,217.50 vs 09:30 $10,217.50 (session +0.00) | 16:00 close · cash $10,217.50 · no lots left · equity $10,217.50. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.50 | ▲ 09:30 equity $10,217.50 vs yday $10,217.50 (+0.00) | 09:30 open · cash $10,217.50 · no holdings · equity $10,217.50 vs prior close $10,217.50 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,217.50 | ▲ close $10,217.50 vs 09:30 $10,217.50 (session +0.00) | 16:00 close · cash $10,217.50 · no lots left · equity $10,217.50. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.50 | ▲ 09:30 equity $10,217.50 vs yday $10,217.50 (+0.00) | 09:30 open · cash $10,217.50 · no holdings · equity $10,217.50 vs prior close $10,217.50 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,217.50 | ▲ close $10,217.50 vs 09:30 $10,217.50 (session +0.00) | 16:00 close · cash $10,217.50 · no lots left · equity $10,217.50. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.50 | ▲ 09:30 equity $10,217.50 vs yday $10,217.50 (+0.00) | 09:30 open · cash $10,217.50 · no holdings · equity $10,217.50 vs prior close $10,217.50 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 884 | $11.54 | $11.40 | — | $4.74 | — | combo gate; gate flow_in=True,zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $10217.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.74 | ▼ close $10,126.54 vs 09:30 $10,217.50 (session -79.56) | 16:00 close · cash $4.74 · equity $10,126.54 vs 09:30 $10,217.50 (-90.96; session marks -79.56) · 1 name(s) marked open→close (per-name table). VIR×884 09:30 $11.54 → close $11.45 -79.56 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.74 | ▼ 09:30 equity $10,002.78 vs yday $10,126.54 (-123.76) | 09:30 open · cash $4.74 (unchanged overnight, no fees) · equity $10,002.78 vs prior close $10,126.54 (-123.76) · 1 name(s) re-marked at the open (per-name table). VIR×884 yday $11.45 → 09:30 $11.31 -123.76 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.74 | ▲ close $10,069.08 vs 09:30 $10,002.78 (session +66.30) | 16:00 close · cash $4.74 · equity $10,069.08 vs 09:30 $10,002.78 (+66.30; session marks +66.30) · 1 name(s) marked open→close (per-name table). VIR×884 09:30 $11.31 → close $11.38 +66.30 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.74 | ▼ 09:30 equity $9,923.22 vs yday $10,069.08 (-145.86) | 09:30 open · cash $4.74 (unchanged overnight, no fees) · equity $9,923.22 vs prior close $10,069.08 (-145.86) · 1 name(s) re-marked at the open (per-name table). VIR×884 yday $11.38 → 09:30 $11.22 -145.86 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.74 | ▼ close $9,887.86 vs 09:30 $9,923.22 (session -35.36) | 16:00 close · cash $4.74 · equity $9,887.86 vs 09:30 $9,923.22 (-35.36; session marks -35.36) · 1 name(s) marked open→close (per-name table). VIR×884 09:30 $11.22 → close $11.18 -35.36 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.74 | ▼ 09:30 equity $9,764.10 vs yday $9,887.86 (-123.76) | 09:30 open · cash $4.74 (unchanged overnight, no fees) · equity $9,764.10 vs prior close $9,887.86 (-123.76) · 1 name(s) re-marked at the open (per-name table). VIR×884 yday $11.18 → 09:30 $11.04 -123.76 | — |
| 2026-09-09 09:30 ET | **SELL** | `VIR` | 884 | $11.04 | $11.63 | $-465.03 | $9,752.47 | ▼ -465.03 after sell → book $9,752.47; vs 09:30 mark -11.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,752.47 | ▲ close $9,752.47 vs 09:30 $9,764.10 (session +0.00) | 16:00 close · cash $9,752.47 · no lots left · equity $9,752.47. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,752.47 | ▲ 09:30 equity $9,752.47 vs yday $9,752.47 (+0.00) | 09:30 open · cash $9,752.47 · no holdings · equity $9,752.47 vs prior close $9,752.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,752.47 | ▲ close $9,752.47 vs 09:30 $9,752.47 (session +0.00) | 16:00 close · cash $9,752.47 · no lots left · equity $9,752.47. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,752.47 | ▲ 09:30 equity $9,752.47 vs yday $9,752.47 (+0.00) | 09:30 open · cash $9,752.47 · no holdings · equity $9,752.47 vs prior close $9,752.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,752.47 | ▲ close $9,752.47 vs 09:30 $9,752.47 (session +0.00) | 16:00 close · cash $9,752.47 · no lots left · equity $9,752.47. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,752.47 | ▲ 09:30 equity $9,752.47 vs yday $9,752.47 (+0.00) | 09:30 open · cash $9,752.47 · no holdings · equity $9,752.47 vs prior close $9,752.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,752.47 | ▲ close $9,752.47 vs 09:30 $9,752.47 (session +0.00) | 16:00 close · cash $9,752.47 · no lots left · equity $9,752.47. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,752.47 | ▲ 09:30 equity $9,752.47 vs yday $9,752.47 (+0.00) | 09:30 open · cash $9,752.47 · no holdings · equity $9,752.47 vs prior close $9,752.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,752.47 | ▲ close $9,752.47 vs 09:30 $9,752.47 (session +0.00) | 16:00 close · cash $9,752.47 · no lots left · equity $9,752.47. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,752.47 | ▲ 09:30 equity $9,752.47 vs yday $9,752.47 (+0.00) | 09:30 open · cash $9,752.47 · no holdings · equity $9,752.47 vs prior close $9,752.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,752.47 | ▲ close $9,752.47 vs 09:30 $9,752.47 (session +0.00) | 16:00 close · cash $9,752.47 · no lots left · equity $9,752.47. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,752.47 | ▲ 09:30 equity $9,752.47 vs yday $9,752.47 (+0.00) | 09:30 open · cash $9,752.47 · no holdings · equity $9,752.47 vs prior close $9,752.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,752.47 | ▲ close $9,752.47 vs 09:30 $9,752.47 (session +0.00) | 16:00 close · cash $9,752.47 · no lots left · equity $9,752.47. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,752.47 | ▲ 09:30 equity $9,752.47 vs yday $9,752.47 (+0.00) | 09:30 open · cash $9,752.47 · no holdings · equity $9,752.47 vs prior close $9,752.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 09:30 ET | **BUY** | `S` | 421 | $23.13 | $5.43 | — | $9.31 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $9752.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.31 | ▼ close $9,486.02 vs 09:30 $9,752.47 (session -261.02) | 16:00 close · cash $9.31 · equity $9,486.02 vs 09:30 $9,752.47 (-266.45; session marks -261.02) · 1 name(s) marked open→close (per-name table). S×421 09:30 $23.13 → close $22.51 -261.02 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SPHR` | cash | leftover split 3.52 < 1 share @ 176.68 |
| 2026-08-14 | `BSBR` | cash | leftover split 3.52 < 1 share @ 5.79 |
| 2026-08-14 | `ENB` | cash | leftover split 3.52 < 1 share @ 51.15 |
| 2026-08-14 | `RUM` | cash | leftover split 3.52 < 1 share @ 7.51 |
| 2026-08-14 | `JBTM` | cash | leftover split 3.52 < 1 share @ 117.42 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `KULR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `RLX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VIV` | cash | leftover split 10.12 < 1 share @ 11.55 |
| 2026-08-17 | `WBS` | cash | leftover split 10.12 < 1 share @ 79.00 |
| 2026-08-18 | `KULR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `RLX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `WBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `WBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HITI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `WBS` | no_price | no 09:30 open — carry |
| 2026-08-25 | `HITI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `RHI` | cash | leftover split 0.81 < 1 share @ 43.76 |
| 2026-08-25 | `ABUS` | cash | leftover split 0.81 < 1 share @ 5.25 |
| 2026-08-25 | `DBRG` | cash | leftover split 0.81 < 1 share @ 15.98 |
| 2026-08-26 | `WBS` | no_price | no 09:30 open — carry |
| 2026-08-27 | `WBS` | no_price | no 09:30 open — carry |
| 2026-09-04 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ATRC` | cash | leftover split 1.18 < 1 share @ 52.03 |
| 2026-09-04 | `ADCT` | cash | leftover split 1.18 < 1 share @ 1.30 |
| 2026-09-04 | `XP` | cash | leftover split 1.18 < 1 share @ 19.67 |
| 2026-09-04 | `MMED` | cash | leftover split 1.18 < 1 share @ 23.84 |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `S` | 421 | 2026-09-18 @ $23.13 | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $9752.47 |
