# Factor mine action — `union_flow_in_white_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+2.10%** ($10,210) · signal-only (no cash/fees) was +4.80%. Starts YES **2/30**. Fills 12 · skips 18 · realized $+210.43.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,210.43.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TPG` | 197 | — | $50.62 | +0.00 | $54.62 | +787.37 | +787.37 | +0.00 | +787.37 |
| 2026-08-14 | `TPG` | 197 | $54.62 | $55.29 | +131.99 | $53.03 | -445.22 | -313.23 | +919.36 | +474.14 |
| 2026-08-14 | `KULR` | 3 | — | $2.50 | +0.00 | $2.64 | +0.42 | +0.42 | +0.00 | +0.42 |
| 2026-08-14 | `RLX` | 4 | — | $1.85 | +0.00 | $1.94 | +0.36 | +0.36 | +0.00 | +0.36 |
| 2026-08-17 | `TPG` | 197 | $53.03 | $52.67 | -70.92 | $51.77 | -177.30 | -248.22 | +403.22 | +225.92 |
| 2026-08-17 | `KULR` | 3 | $2.64 | $2.63 | -0.03 | $2.62 | -0.03 | -0.06 | +0.39 | +0.36 |
| 2026-08-17 | `RLX` | 4 | $1.94 | $1.92 | -0.08 | $1.87 | -0.20 | -0.28 | +0.28 | +0.08 |
| 2026-08-18 | `TPG` | 197 | $51.77 | $51.77 | +0.00 | — | +0.00 | +0.00 | +225.92 | — |
| 2026-08-18 | `KULR` | 3 | $2.62 | $2.53 | -0.27 | $2.57 | +0.12 | -0.15 | +0.09 | +0.21 |
| 2026-08-18 | `RLX` | 4 | $1.87 | $1.86 | -0.04 | $1.83 | -0.12 | -0.16 | +0.04 | -0.08 |
| 2026-08-19 | `KULR` | 3 | $2.57 | $2.55 | -0.06 | — | +0.00 | -0.06 | +0.15 | — |
| 2026-08-19 | `RLX` | 4 | $1.83 | $1.84 | +0.04 | — | +0.00 | +0.04 | -0.04 | — |
| 2026-08-20 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-21 | `HITI` | 4183 | — | $2.43 | +0.00 | $2.45 | +83.66 | +83.66 | +0.00 | +83.66 |
| 2026-08-24 | `HITI` | 4183 | $2.45 | $2.45 | +0.00 | $2.46 | +41.83 | +41.83 | +83.66 | +125.49 |
| 2026-08-25 | `HITI` | 4183 | $2.46 | $2.48 | +83.66 | $2.57 | +376.47 | +460.13 | +209.15 | +585.62 |
| 2026-08-26 | `HITI` | 4183 | $2.57 | $2.57 | +0.00 | — | +0.00 | +0.00 | +585.62 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `VIR` | 925 | — | $11.54 | +0.00 | $11.45 | -83.25 | -83.25 | +0.00 | -83.25 |
| 2026-09-04 | `VIR` | 925 | $11.45 | $11.31 | -129.50 | $11.38 | +69.37 | -60.13 | -212.75 | -143.37 |
| 2026-09-04 | `ADCT` | 2 | — | $1.30 | +0.00 | $1.36 | +0.12 | +0.12 | +0.00 | +0.12 |
| 2026-09-08 | `VIR` | 925 | $11.38 | $11.22 | -152.62 | $11.18 | -37.00 | -189.62 | -296.00 | -333.00 |
| 2026-09-08 | `ADCT` | 2 | $1.36 | $1.33 | -0.06 | $1.30 | -0.06 | -0.12 | +0.06 | +0.00 |
| 2026-09-09 | `VIR` | 925 | $11.18 | $11.04 | -129.50 | — | +0.00 | -129.50 | -462.50 | — |
| 2026-09-09 | `ADCT` | 2 | $1.30 | $1.28 | -0.04 | $1.22 | -0.12 | -0.16 | -0.04 | -0.16 |
| 2026-09-10 | `ADCT` | 2 | $1.22 | $1.21 | -0.02 | — | +0.00 | -0.02 | -0.18 | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-21 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-22 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-23 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +787.37 | TPG | — | $24.65 | $10,784.79 | TPG×197 |
| 2026-08-14 | +5.50 | $24.65 | TPG×197 | $10,916.78 | +131.99 | -444.44 | KULR, RLX | — | $9.58 | $10,472.17 | TPG×197, KULR×3, RLX×4 |
| 2026-08-17 | +2.25 | $9.58 | TPG×197, KULR×3, RLX×4 | $10,401.14 | -71.03 | -177.53 | — | — | $9.58 | $10,223.61 | TPG×197, KULR×3, RLX×4 |
| 2026-08-18 | -6.20 | $9.58 | TPG×197, KULR×3, RLX×4 | $10,223.30 | -0.31 | +0.00 | — | TPG | $10,205.57 | $10,220.60 | KULR×3, RLX×4 |
| 2026-08-19 | -7.20 | $10,205.57 | KULR×3, RLX×4 | $10,220.58 | -0.02 | +0.00 | — | KULR, RLX | $10,220.37 | $10,220.37 | — |
| 2026-08-20 | +1.12 | $10,220.37 | — | $10,220.37 | +0.00 | +0.00 | — | — | $10,220.37 | $10,220.37 | — |
| 2026-08-21 | +3.25 | $10,220.37 | — | $10,220.37 | +0.00 | +83.66 | HITI | — | $1.72 | $10,250.07 | HITI×4183 |
| 2026-08-24 | -5.17 | $1.72 | HITI×4183 | $10,250.07 | +0.00 | +41.83 | — | — | $1.72 | $10,291.90 | HITI×4183 |
| 2026-08-25 | +1.80 | $1.72 | HITI×4183 | $10,375.56 | +83.66 | +376.47 | — | — | $1.72 | $10,752.03 | HITI×4183 |
| 2026-08-26 | +2.02 | $1.72 | HITI×4183 | $10,752.03 | +0.00 | +0.00 | — | HITI | $10,697.29 | $10,697.29 | — |
| 2026-08-27 | — | $10,697.29 | — | $10,697.29 | +0.00 | +0.00 | — | — | $10,697.29 | $10,697.29 | — |
| 2026-08-28 | +0.75 | $10,697.29 | — | $10,697.29 | +0.00 | +0.00 | — | — | $10,697.29 | $10,697.29 | — |
| 2026-08-31 | -5.85 | $10,697.29 | — | $10,697.29 | +0.00 | +0.00 | — | — | $10,697.29 | $10,697.29 | — |
| 2026-09-01 | -6.30 | $10,697.29 | — | $10,697.29 | +0.00 | +0.00 | — | — | $10,697.29 | $10,697.29 | — |
| 2026-09-02 | -3.83 | $10,697.29 | — | $10,697.29 | +0.00 | +0.00 | — | — | $10,697.29 | $10,697.29 | — |
| 2026-09-03 | -0.90 | $10,697.29 | — | $10,697.29 | +0.00 | -83.25 | VIR | — | $10.86 | $10,602.11 | VIR×925 |
| 2026-09-04 | +2.25 | $10.86 | VIR×925 | $10,472.61 | -129.50 | +69.49 | ADCT | — | $8.23 | $10,542.07 | VIR×925, ADCT×2 |
| 2026-09-08 | -11.47 | $8.23 | VIR×925, ADCT×2 | $10,389.39 | -152.68 | -37.06 | — | — | $8.23 | $10,352.33 | VIR×925, ADCT×2 |
| 2026-09-09 | -13.95 | $8.23 | VIR×925, ADCT×2 | $10,222.79 | -129.54 | -0.12 | — | VIR | $10,208.06 | $10,210.50 | ADCT×2 |
| 2026-09-10 | -13.28 | $10,208.06 | ADCT×2 | $10,210.48 | -0.02 | +0.00 | — | ADCT | $10,210.43 | $10,210.43 | — |
| 2026-09-11 | +0.50 | $10,210.43 | — | $10,210.43 | -0.00 | +0.00 | — | — | $10,210.43 | $10,210.43 | — |
| 2026-09-14 | -11.00 | $10,210.43 | — | $10,210.43 | -0.00 | +0.00 | — | — | $10,210.43 | $10,210.43 | — |
| 2026-09-15 | -3.84 | $10,210.43 | — | $10,210.43 | -0.00 | +0.00 | — | — | $10,210.43 | $10,210.43 | — |
| 2026-09-16 | +5.30 | $10,210.43 | — | $10,210.43 | -0.00 | +0.00 | — | — | $10,210.43 | $10,210.43 | — |
| 2026-09-17 | +7.38 | $10,210.43 | — | $10,210.43 | -0.00 | +0.00 | — | — | $10,210.43 | $10,210.43 | — |
| 2026-09-18 | +4.86 | $10,210.43 | — | $10,210.43 | -0.00 | +0.00 | — | — | $10,210.43 | $10,210.43 | — |
| 2026-09-21 | +12.87 | $10,210.43 | — | $10,210.43 | -0.00 | +0.00 | — | — | $10,210.43 | $10,210.43 | — |
| 2026-09-22 | -0.50 | $10,210.43 | — | $10,210.43 | -0.00 | +0.00 | — | — | $10,210.43 | $10,210.43 | — |
| 2026-09-23 | +2.29 | $10,210.43 | — | $10,210.43 | -0.00 | +0.00 | — | — | $10,210.43 | $10,210.43 | — |
| 2026-09-24 | -7.66 | $10,210.43 | — | $10,210.43 | -0.00 | +0.00 | — | — | $10,210.43 | $10,210.43 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 197 | $50.62 | $2.58 | — | $24.65 | — | combo gate; gate flow_in=True,zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.65 | ▲ close $10,784.79 vs 09:30 $10,000.00 (session +787.37) | 16:00 close · cash $24.65 · equity $10,784.79 vs 09:30 $10,000.00 (+784.79; session marks +787.37) · 1 name(s) marked open→close (per-name table). TPG×197 09:30 $50.62 → close $54.62 +787.37 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.65 | ▲ 09:30 equity $10,916.78 vs yday $10,784.79 (+131.99) | 09:30 open · cash $24.65 (unchanged overnight, no fees) · equity $10,916.78 vs prior close $10,784.79 (+131.99) · 1 name(s) re-marked at the open (per-name table). TPG×197 yday $54.62 → 09:30 $55.29 +131.99 | — |
| 2026-08-14 09:30 ET | **BUY** | `KULR` | 3 | $2.50 | $0.08 | — | $17.06 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+7.6; leftover $8.22 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `RLX` | 4 | $1.85 | $0.09 | — | $9.58 | — | combo gate; gate flow_in=True,zero_red=True; list earn_react; 🔵; ⚪; ret5=+0.5; leftover $8.22 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.58 | ▼ close $10,472.17 vs 09:30 $10,916.78 (session -444.44) | 16:00 close · cash $9.58 · equity $10,472.17 vs 09:30 $10,916.78 (-444.61; session marks -444.44) · 3 name(s) marked open→close (per-name table). TPG×197 09:30 $55.29 → close $53.03 -445.22; KULR×3 09:30 $2.50 → close $2.64 +0.42; RLX×4 09:30 $1.85 → close $1.94 +0.36 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.58 | ▼ 09:30 equity $10,401.14 vs yday $10,472.17 (-71.03) | 09:30 open · cash $9.58 (unchanged overnight, no fees) · equity $10,401.14 vs prior close $10,472.17 (-71.03) · 3 name(s) re-marked at the open (per-name table). TPG×197 yday $53.03 → 09:30 $52.67 -70.92; KULR×3 yday $2.64 → 09:30 $2.63 -0.03; RLX×4 yday $1.94 → 09:30 $1.92 -0.08 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.58 | ▼ close $10,223.61 vs 09:30 $10,401.14 (session -177.53) | 16:00 close · cash $9.58 · equity $10,223.61 vs 09:30 $10,401.14 (-177.53; session marks -177.53) · 3 name(s) marked open→close (per-name table). TPG×197 09:30 $52.67 → close $51.77 -177.30; KULR×3 09:30 $2.63 → close $2.62 -0.03; RLX×4 09:30 $1.92 → close $1.87 -0.20 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.58 | ▼ 09:30 equity $10,223.30 vs yday $10,223.61 (-0.31) | 09:30 open · cash $9.58 (unchanged overnight, no fees) · equity $10,223.30 vs prior close $10,223.61 (-0.31) · 3 name(s) re-marked at the open (per-name table). TPG×197 yday $51.77 → 09:30 $51.77 +0.00; KULR×3 yday $2.62 → 09:30 $2.53 -0.27; RLX×4 yday $1.87 → 09:30 $1.86 -0.04 | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 197 | $51.77 | $2.70 | $+220.64 | $10,205.57 | ▲ +220.64 after sell → book $10,220.60; vs 09:30 mark -2.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,205.57 | ▲ close $10,220.60 vs 09:30 $10,223.30 (session +0.00) | 16:00 close · cash $10,205.57 · equity $10,220.60 vs 09:30 $10,223.30 (-2.70; session marks +0.00) · 2 name(s) marked open→close (per-name table). KULR×3 09:30 $2.53 → close $2.57 +0.12; RLX×4 09:30 $1.86 → close $1.83 -0.12 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,205.57 | ▼ 09:30 equity $10,220.58 vs yday $10,220.60 (-0.02) | 09:30 open · cash $10,205.57 (unchanged overnight, no fees) · equity $10,220.58 vs prior close $10,220.60 (-0.02) · 2 name(s) re-marked at the open (per-name table). KULR×3 yday $2.57 → 09:30 $2.55 -0.06; RLX×4 yday $1.83 → 09:30 $1.84 +0.04 | — |
| 2026-08-19 09:30 ET | **SELL** | `KULR` | 3 | $2.55 | $0.11 | $-0.04 | $10,213.12 | ▼ -0.04 after sell → book $10,220.48; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `RLX` | 4 | $1.84 | $0.11 | $-0.23 | $10,220.37 | ▼ -0.23 after sell → book $10,220.37; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,220.37 | ▲ close $10,220.37 vs 09:30 $10,220.58 (session +0.00) | 16:00 close · cash $10,220.37 · no lots left · equity $10,220.37. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,220.37 | ▲ 09:30 equity $10,220.37 vs yday $10,220.37 (+0.00) | 09:30 open · cash $10,220.37 · no holdings · equity $10,220.37 vs prior close $10,220.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,220.37 | ▲ close $10,220.37 vs 09:30 $10,220.37 (session +0.00) | 16:00 close · cash $10,220.37 · no lots left · equity $10,220.37. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,220.37 | ▲ 09:30 equity $10,220.37 vs yday $10,220.37 (+0.00) | 09:30 open · cash $10,220.37 · no holdings · equity $10,220.37 vs prior close $10,220.37 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 4183 | $2.43 | $53.96 | — | $1.72 | — | combo gate; gate flow_in=True,zero_red=True; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $10220.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.72 | ▲ close $10,250.07 vs 09:30 $10,220.37 (session +83.66) | 16:00 close · cash $1.72 · equity $10,250.07 vs 09:30 $10,220.37 (+29.70; session marks +83.66) · 1 name(s) marked open→close (per-name table). HITI×4183 09:30 $2.43 → close $2.45 +83.66 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.72 | ▲ 09:30 equity $10,250.07 vs yday $10,250.07 (+0.00) | 09:30 open · cash $1.72 (unchanged overnight, no fees) · equity $10,250.07 vs prior close $10,250.07 (+0.00) · 1 name(s) re-marked at the open (per-name table). HITI×4183 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.72 | ▲ close $10,291.90 vs 09:30 $10,250.07 (session +41.83) | 16:00 close · cash $1.72 · equity $10,291.90 vs 09:30 $10,250.07 (+41.83; session marks +41.83) · 1 name(s) marked open→close (per-name table). HITI×4183 09:30 $2.45 → close $2.46 +41.83 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.72 | ▲ 09:30 equity $10,375.56 vs yday $10,291.90 (+83.66) | 09:30 open · cash $1.72 (unchanged overnight, no fees) · equity $10,375.56 vs prior close $10,291.90 (+83.66) · 1 name(s) re-marked at the open (per-name table). HITI×4183 yday $2.46 → 09:30 $2.48 +83.66 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.72 | ▲ close $10,752.03 vs 09:30 $10,375.56 (session +376.47) | 16:00 close · cash $1.72 · equity $10,752.03 vs 09:30 $10,375.56 (+376.47; session marks +376.47) · 1 name(s) marked open→close (per-name table). HITI×4183 09:30 $2.48 → close $2.57 +376.47 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.72 | ▲ 09:30 equity $10,752.03 vs yday $10,752.03 (+0.00) | 09:30 open · cash $1.72 (unchanged overnight, no fees) · equity $10,752.03 vs prior close $10,752.03 (+0.00) · 1 name(s) re-marked at the open (per-name table). HITI×4183 yday $2.57 → 09:30 $2.57 +0.00 | — |
| 2026-08-26 09:30 ET | **SELL** | `HITI` | 4183 | $2.57 | $54.74 | $+476.92 | $10,697.29 | ▲ +476.92 after sell → book $10,697.29; vs 09:30 mark -54.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,697.29 | ▲ close $10,697.29 vs 09:30 $10,752.03 (session +0.00) | 16:00 close · cash $10,697.29 · no lots left · equity $10,697.29. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,697.29 | ▲ 09:30 equity $10,697.29 vs yday $10,697.29 (+0.00) | 09:30 open · cash $10,697.29 · no holdings · equity $10,697.29 vs prior close $10,697.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,697.29 | ▲ close $10,697.29 vs 09:30 $10,697.29 (session +0.00) | 16:00 close · cash $10,697.29 · no lots left · equity $10,697.29. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,697.29 | ▲ 09:30 equity $10,697.29 vs yday $10,697.29 (+0.00) | 09:30 open · cash $10,697.29 · no holdings · equity $10,697.29 vs prior close $10,697.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,697.29 | ▲ close $10,697.29 vs 09:30 $10,697.29 (session +0.00) | 16:00 close · cash $10,697.29 · no lots left · equity $10,697.29. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,697.29 | ▲ 09:30 equity $10,697.29 vs yday $10,697.29 (+0.00) | 09:30 open · cash $10,697.29 · no holdings · equity $10,697.29 vs prior close $10,697.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,697.29 | ▲ close $10,697.29 vs 09:30 $10,697.29 (session +0.00) | 16:00 close · cash $10,697.29 · no lots left · equity $10,697.29. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,697.29 | ▲ 09:30 equity $10,697.29 vs yday $10,697.29 (+0.00) | 09:30 open · cash $10,697.29 · no holdings · equity $10,697.29 vs prior close $10,697.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,697.29 | ▲ close $10,697.29 vs 09:30 $10,697.29 (session +0.00) | 16:00 close · cash $10,697.29 · no lots left · equity $10,697.29. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,697.29 | ▲ 09:30 equity $10,697.29 vs yday $10,697.29 (+0.00) | 09:30 open · cash $10,697.29 · no holdings · equity $10,697.29 vs prior close $10,697.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,697.29 | ▲ close $10,697.29 vs 09:30 $10,697.29 (session +0.00) | 16:00 close · cash $10,697.29 · no lots left · equity $10,697.29. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,697.29 | ▲ 09:30 equity $10,697.29 vs yday $10,697.29 (+0.00) | 09:30 open · cash $10,697.29 · no holdings · equity $10,697.29 vs prior close $10,697.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 925 | $11.54 | $11.93 | — | $10.86 | — | combo gate; gate flow_in=True,zero_red=True; list yday_gainer; 🔵; ⚪; ret5=+3.8; leftover $10697.29 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.86 | ▼ close $10,602.11 vs 09:30 $10,697.29 (session -83.25) | 16:00 close · cash $10.86 · equity $10,602.11 vs 09:30 $10,697.29 (-95.18; session marks -83.25) · 1 name(s) marked open→close (per-name table). VIR×925 09:30 $11.54 → close $11.45 -83.25 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.86 | ▼ 09:30 equity $10,472.61 vs yday $10,602.11 (-129.50) | 09:30 open · cash $10.86 (unchanged overnight, no fees) · equity $10,472.61 vs prior close $10,602.11 (-129.50) · 1 name(s) re-marked at the open (per-name table). VIR×925 yday $11.45 → 09:30 $11.31 -129.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `ADCT` | 2 | $1.30 | $0.03 | — | $8.23 | — | combo gate; gate flow_in=True,zero_red=True; list ohlc_hot; 🔵; ⚪; ret5=+17.9; leftover $3.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.23 | ▲ close $10,542.07 vs 09:30 $10,472.61 (session +69.49) | 16:00 close · cash $8.23 · equity $10,542.07 vs 09:30 $10,472.61 (+69.46; session marks +69.49) · 2 name(s) marked open→close (per-name table). VIR×925 09:30 $11.31 → close $11.38 +69.37; ADCT×2 09:30 $1.30 → close $1.36 +0.12 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.23 | ▼ 09:30 equity $10,389.39 vs yday $10,542.07 (-152.68) | 09:30 open · cash $8.23 (unchanged overnight, no fees) · equity $10,389.39 vs prior close $10,542.07 (-152.68) · 2 name(s) re-marked at the open (per-name table). VIR×925 yday $11.38 → 09:30 $11.22 -152.62; ADCT×2 yday $1.36 → 09:30 $1.33 -0.06 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.23 | ▼ close $10,352.33 vs 09:30 $10,389.39 (session -37.06) | 16:00 close · cash $8.23 · equity $10,352.33 vs 09:30 $10,389.39 (-37.06; session marks -37.06) · 2 name(s) marked open→close (per-name table). VIR×925 09:30 $11.22 → close $11.18 -37.00; ADCT×2 09:30 $1.33 → close $1.30 -0.06 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.23 | ▼ 09:30 equity $10,222.79 vs yday $10,352.33 (-129.54) | 09:30 open · cash $8.23 (unchanged overnight, no fees) · equity $10,222.79 vs prior close $10,352.33 (-129.54) · 2 name(s) re-marked at the open (per-name table). VIR×925 yday $11.18 → 09:30 $11.04 -129.50; ADCT×2 yday $1.30 → 09:30 $1.28 -0.04 | — |
| 2026-09-09 09:30 ET | **SELL** | `VIR` | 925 | $11.04 | $12.17 | $-486.60 | $10,208.06 | ▼ -486.60 after sell → book $10,210.62; vs 09:30 mark -12.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,208.06 | ▼ close $10,210.50 vs 09:30 $10,222.79 (session -0.12) | 16:00 close · cash $10,208.06 · equity $10,210.50 vs 09:30 $10,222.79 (-12.29; session marks -0.12) · 1 name(s) marked open→close (per-name table). ADCT×2 09:30 $1.28 → close $1.22 -0.12 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,208.06 | ▼ 09:30 equity $10,210.48 vs yday $10,210.50 (-0.02) | 09:30 open · cash $10,208.06 (unchanged overnight, no fees) · equity $10,210.48 vs prior close $10,210.50 (-0.02) · 1 name(s) re-marked at the open (per-name table). ADCT×2 yday $1.22 → 09:30 $1.21 -0.02 | — |
| 2026-09-10 09:30 ET | **SELL** | `ADCT` | 2 | $1.21 | $0.05 | $-0.26 | $10,210.43 | ▼ -0.26 after sell → book $10,210.43; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.48 (session +0.00) | 16:00 close · cash $10,210.43 · no lots left · equity $10,210.43. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | 09:30 open · cash $10,210.43 · no holdings · equity $10,210.43 vs prior close $10,210.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | 16:00 close · cash $10,210.43 · no lots left · equity $10,210.43. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | 09:30 open · cash $10,210.43 · no holdings · equity $10,210.43 vs prior close $10,210.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | 16:00 close · cash $10,210.43 · no lots left · equity $10,210.43. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | 09:30 open · cash $10,210.43 · no holdings · equity $10,210.43 vs prior close $10,210.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | 16:00 close · cash $10,210.43 · no lots left · equity $10,210.43. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | 09:30 open · cash $10,210.43 · no holdings · equity $10,210.43 vs prior close $10,210.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | 16:00 close · cash $10,210.43 · no lots left · equity $10,210.43. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | 09:30 open · cash $10,210.43 · no holdings · equity $10,210.43 vs prior close $10,210.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | 16:00 close · cash $10,210.43 · no lots left · equity $10,210.43. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | 09:30 open · cash $10,210.43 · no holdings · equity $10,210.43 vs prior close $10,210.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | 16:00 close · cash $10,210.43 · no lots left · equity $10,210.43. | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | 09:30 open · cash $10,210.43 · no holdings · equity $10,210.43 vs prior close $10,210.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | 16:00 close · cash $10,210.43 · no lots left · equity $10,210.43. | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | 09:30 open · cash $10,210.43 · no holdings · equity $10,210.43 vs prior close $10,210.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | 16:00 close · cash $10,210.43 · no lots left · equity $10,210.43. | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | 09:30 open · cash $10,210.43 · no holdings · equity $10,210.43 vs prior close $10,210.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | 16:00 close · cash $10,210.43 · no lots left · equity $10,210.43. | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,210.43 | ▲ 09:30 equity $10,210.43 vs yday $10,210.43 (-0.00) | 09:30 open · cash $10,210.43 · no holdings · equity $10,210.43 vs prior close $10,210.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,210.43 | ▲ close $10,210.43 vs 09:30 $10,210.43 (session +0.00) | 16:00 close · cash $10,210.43 · no lots left · equity $10,210.43. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SPHR` | cash | leftover split 8.22 < 1 share @ 176.68 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `KULR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `RLX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `XP` | cash | leftover split 9.58 < 1 share @ 15.93 |
| 2026-08-18 | `KULR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `RLX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HITI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `HITI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `RHI` | cash | leftover split 0.86 < 1 share @ 43.76 |
| 2026-08-25 | `ABUS` | cash | leftover split 0.86 < 1 share @ 5.25 |
| 2026-09-04 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ATRC` | cash | leftover split 3.62 < 1 share @ 52.03 |
| 2026-09-04 | `XP` | cash | leftover split 3.62 < 1 share @ 19.67 |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ADCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `ADCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
