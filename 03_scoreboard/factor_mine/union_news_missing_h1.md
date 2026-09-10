# Factor mine action — `union_news_missing_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_missing, no 🚨

Cash book **+1.64%** ($10,164) · signal-only (no cash/fees) was +2.18%. Starts YES **11/19**. Fills 32 · skips 0 · realized $+164.19.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera (does the morning packet like the headline?) is blank.
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
- **Gate** `news=missing` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,164.19.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-13 | `TGTX` | 25 | — | $49.70 | +0.00 | $47.94 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | — | +0.00 | +16.08 | +112.00 | — |
| 2026-08-14 | `TGTX` | 25 | $47.94 | $47.27 | -16.75 | — | +0.00 | -16.75 | -60.75 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | — | +0.00 | +15.96 | -24.78 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-21 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-27 | `CRK` | 87 | — | $14.42 | +0.00 | $14.62 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-27 | `MOS` | 52 | — | $24.00 | +0.00 | $23.76 | -12.48 | -12.48 | +0.00 | -12.48 |
| 2026-08-27 | `SLI` | 487 | — | $2.60 | +0.00 | $2.64 | +19.48 | +19.48 | +0.00 | +19.48 |
| 2026-08-27 | `GGB` | 277 | — | $4.57 | +0.00 | $4.70 | +36.01 | +36.01 | +0.00 | +36.01 |
| 2026-08-27 | `MT` | 17 | — | $74.54 | +0.00 | $74.63 | +1.53 | +1.53 | +0.00 | +1.53 |
| 2026-08-27 | `TX` | 22 | — | $55.25 | +0.00 | $55.83 | +12.76 | +12.76 | +0.00 | +12.76 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `DLO` | 82 | — | $15.33 | +0.00 | $15.14 | -15.58 | -15.58 | +0.00 | -15.58 |
| 2026-08-28 | `CRK` | 87 | $14.62 | $14.63 | +0.87 | — | +0.00 | +0.87 | +18.27 | — |
| 2026-08-28 | `MOS` | 52 | $23.76 | $23.95 | +9.88 | — | +0.00 | +9.88 | -2.60 | — |
| 2026-08-28 | `SLI` | 487 | $2.64 | $2.68 | +19.48 | — | +0.00 | +19.48 | +38.96 | — |
| 2026-08-28 | `GGB` | 277 | $4.70 | $4.67 | -8.31 | — | +0.00 | -8.31 | +27.70 | — |
| 2026-08-28 | `MT` | 17 | $74.63 | $75.39 | +12.92 | — | +0.00 | +12.92 | +14.45 | — |
| 2026-08-28 | `TX` | 22 | $55.83 | $55.97 | +3.08 | — | +0.00 | +3.08 | +15.84 | — |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | — | +0.00 | -6.54 | -35.40 | — |
| 2026-08-28 | `DLO` | 82 | $15.14 | $15.19 | +4.10 | — | +0.00 | +4.10 | -11.48 | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-08 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +185.07 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | +0.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $10,143.91 | $10,143.91 | — |
| 2026-08-17 | +2.25 | $10,143.91 | — | $10,143.91 | -0.00 | +0.00 | — | — | $10,143.91 | $10,143.91 | — |
| 2026-08-18 | -6.20 | $10,143.91 | — | $10,143.91 | -0.00 | +0.00 | — | — | $10,143.91 | $10,143.91 | — |
| 2026-08-19 | -7.20 | $10,143.91 | — | $10,143.91 | -0.00 | +0.00 | — | — | $10,143.91 | $10,143.91 | — |
| 2026-08-20 | +1.12 | $10,143.91 | — | $10,143.91 | -0.00 | +0.00 | — | — | $10,143.91 | $10,143.91 | — |
| 2026-08-21 | +3.25 | $10,143.91 | — | $10,143.91 | -0.00 | +0.00 | — | — | $10,143.91 | $10,143.91 | — |
| 2026-08-24 | -5.17 | $10,143.91 | — | $10,143.91 | -0.00 | +0.00 | — | — | $10,143.91 | $10,143.91 | — |
| 2026-08-25 | +1.80 | $10,143.91 | — | $10,143.91 | -0.00 | +0.00 | — | — | $10,143.91 | $10,143.91 | — |
| 2026-08-26 | +2.02 | $10,143.91 | — | $10,143.91 | -0.00 | +0.00 | — | — | $10,143.91 | $10,143.91 | — |
| 2026-08-27 | — | $10,143.91 | — | $10,143.91 | -0.00 | +30.26 | CRK, MOS, SLI, GGB, MT, TX, ANET, DLO | — | $111.55 | $10,151.58 | CRK×87, MOS×52, SLI×487, GGB×277, MT×17, TX×22, ANET×6, DLO×82 |
| 2026-08-28 | +0.75 | $111.55 | CRK×87, MOS×52, SLI×487, GGB×277, MT×17, TX×22, ANET×6, DLO×82 | $10,187.06 | +35.48 | +0.00 | — | CRK, MOS, SLI, GGB, MT, TX, ANET, DLO | $10,164.19 | $10,164.19 | — |
| 2026-08-31 | -5.85 | $10,164.19 | — | $10,164.19 | -0.00 | +0.00 | — | — | $10,164.19 | $10,164.19 | — |
| 2026-09-01 | -6.30 | $10,164.19 | — | $10,164.19 | -0.00 | +0.00 | — | — | $10,164.19 | $10,164.19 | — |
| 2026-09-02 | -3.83 | $10,164.19 | — | $10,164.19 | -0.00 | +0.00 | — | — | $10,164.19 | $10,164.19 | — |
| 2026-09-03 | -0.90 | $10,164.19 | — | $10,164.19 | -0.00 | +0.00 | — | — | $10,164.19 | $10,164.19 | — |
| 2026-09-04 | +2.25 | $10,164.19 | — | $10,164.19 | -0.00 | +0.00 | — | — | $10,164.19 | $10,164.19 | — |
| 2026-09-08 | -11.47 | $10,164.19 | — | $10,164.19 | -0.00 | +0.00 | — | — | $10,164.19 | $10,164.19 | — |
| 2026-09-09 | -13.95 | $10,164.19 | — | $10,164.19 | -0.00 | +0.00 | — | — | $10,164.19 | $10,164.19 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | 16:00 close · cash $97.53 · equity $10,153.12 vs 09:30 $10,000.00 (+153.12; session marks +185.07) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $59.80 → close $60.23 +8.60; IREN×27 09:30 $45.98 → close $44.76 -32.94; TPG×24 09:30 $50.62 → close $54.62 +95.92; TGTX×25 09:30 $49.70 → close $47.94 -44.00; SLS×106 09:30 $11.70 → close $12.36 +69.96; HIMS×42 09:30 $29.74 → close $28.77 -40.74; INO×1543 09:30 $0.81 → close $0.90 +138.87; TNDM×53 09:30 $23.33 → close $23.13 -10.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▼ -7.12 after sell → book $10,176.05; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▼ -55.19 after sell → book $10,173.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ +107.86 after sell → book $10,171.88; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▼ -64.90 after sell → book $10,169.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ +69.56 after sell → book $10,167.46; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▼ -29.03 after sell → book $10,165.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ +148.79 after sell → book $10,146.08; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▼ -26.05 after sell → book $10,143.91; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,143.91 | ▲ close $10,143.91 vs 09:30 $10,178.12 (session +0.00) | 16:00 close · cash $10,143.91 · no lots left · equity $10,143.91. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,143.91 | ▲ close $10,143.91 vs 09:30 $10,143.91 (session +0.00) | 16:00 close · cash $10,143.91 · no lots left · equity $10,143.91. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,143.91 | ▲ close $10,143.91 vs 09:30 $10,143.91 (session +0.00) | 16:00 close · cash $10,143.91 · no lots left · equity $10,143.91. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,143.91 | ▲ close $10,143.91 vs 09:30 $10,143.91 (session +0.00) | 16:00 close · cash $10,143.91 · no lots left · equity $10,143.91. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,143.91 | ▲ close $10,143.91 vs 09:30 $10,143.91 (session +0.00) | 16:00 close · cash $10,143.91 · no lots left · equity $10,143.91. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,143.91 | ▲ close $10,143.91 vs 09:30 $10,143.91 (session +0.00) | 16:00 close · cash $10,143.91 · no lots left · equity $10,143.91. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,143.91 | ▲ close $10,143.91 vs 09:30 $10,143.91 (session +0.00) | 16:00 close · cash $10,143.91 · no lots left · equity $10,143.91. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,143.91 | ▲ close $10,143.91 vs 09:30 $10,143.91 (session +0.00) | 16:00 close · cash $10,143.91 · no lots left · equity $10,143.91. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,143.91 | ▲ close $10,143.91 vs 09:30 $10,143.91 (session +0.00) | 16:00 close · cash $10,143.91 · no lots left · equity $10,143.91. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,143.91 | ▲ 09:30 equity $10,143.91 vs yday $10,143.91 (-0.00) | 09:30 open · cash $10,143.91 · no holdings · equity $10,143.91 vs prior close $10,143.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 87 | $14.42 | $2.25 | — | $8,887.12 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ret5=+7.1; leftover $1267.99 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 52 | $24.00 | $2.15 | — | $7,636.97 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ret5=+8.7; leftover $1267.99 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 487 | $2.60 | $6.28 | — | $6,364.49 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ret5=+13.0; leftover $1267.99 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 277 | $4.57 | $3.57 | — | $5,095.03 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=+1.1; leftover $1267.99 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $74.54 | $2.04 | — | $3,825.81 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=-0.1; leftover $1267.99 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 22 | $55.25 | $2.06 | — | $2,608.25 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=+2.1; leftover $1267.99 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $1,370.84 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=+8.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 82 | $15.33 | $2.24 | — | $111.55 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=+7.4; leftover $1267.99 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.55 | ▲ close $10,151.58 vs 09:30 $10,143.91 (session +30.26) | 16:00 close · cash $111.55 · equity $10,151.58 vs 09:30 $10,143.91 (+7.67; session marks +30.26) · 8 name(s) marked open→close (per-name table). CRK×87 09:30 $14.42 → close $14.62 +17.40; MOS×52 09:30 $24.00 → close $23.76 -12.48; SLI×487 09:30 $2.60 → close $2.64 +19.48; GGB×277 09:30 $4.57 → close $4.70 +36.01; MT×17 09:30 $74.54 → close $74.63 +1.53; TX×22 09:30 $55.25 → close $55.83 +12.76; ANET×6 09:30 $205.90 → close $201.09 -28.86; DLO×82 09:30 $15.33 → close $15.14 -15.58 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.55 | ▲ 09:30 equity $10,187.06 vs yday $10,151.58 (+35.48) | 09:30 open · cash $111.55 (unchanged overnight, no fees) · equity $10,187.06 vs prior close $10,151.58 (+35.48) · 8 name(s) re-marked at the open (per-name table). CRK×87 yday $14.62 → 09:30 $14.63 +0.87; MOS×52 yday $23.76 → 09:30 $23.95 +9.88; SLI×487 yday $2.64 → 09:30 $2.68 +19.48; GGB×277 yday $4.70 → 09:30 $4.67 -8.31; MT×17 yday $74.63 → 09:30 $75.39 +12.92; TX×22 yday $55.83 → 09:30 $55.97 +3.08; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; DLO×82 yday $15.14 → 09:30 $15.19 +4.10 | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 87 | $14.63 | $2.28 | $+13.74 | $1,382.08 | ▲ +13.74 after sell → book $10,184.78; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 52 | $23.95 | $2.17 | $-6.91 | $2,625.31 | ▼ -6.91 after sell → book $10,182.61; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 487 | $2.68 | $6.37 | $+26.30 | $3,924.10 | ▲ +26.30 after sell → book $10,176.24; vs 09:30 mark -6.37 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 277 | $4.67 | $3.63 | $+20.50 | $5,214.06 | ▲ +20.50 after sell → book $10,172.61; vs 09:30 mark -3.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $75.39 | $2.06 | $+10.35 | $6,493.63 | ▲ +10.35 after sell → book $10,170.55; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 22 | $55.97 | $2.08 | $+11.71 | $7,722.89 | ▲ +11.71 after sell → book $10,168.47; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $200.00 | $2.03 | $-39.44 | $8,920.87 | ▼ -39.44 after sell → book $10,166.45; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 82 | $15.19 | $2.26 | $-15.98 | $10,164.19 | ▼ -15.98 after sell → book $10,164.19; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,164.19 | ▲ close $10,164.19 vs 09:30 $10,187.06 (session +0.00) | 16:00 close · cash $10,164.19 · no lots left · equity $10,164.19. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,164.19 | ▲ 09:30 equity $10,164.19 vs yday $10,164.19 (-0.00) | 09:30 open · cash $10,164.19 · no holdings · equity $10,164.19 vs prior close $10,164.19 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,164.19 | ▲ close $10,164.19 vs 09:30 $10,164.19 (session +0.00) | 16:00 close · cash $10,164.19 · no lots left · equity $10,164.19. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,164.19 | ▲ 09:30 equity $10,164.19 vs yday $10,164.19 (-0.00) | 09:30 open · cash $10,164.19 · no holdings · equity $10,164.19 vs prior close $10,164.19 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,164.19 | ▲ close $10,164.19 vs 09:30 $10,164.19 (session +0.00) | 16:00 close · cash $10,164.19 · no lots left · equity $10,164.19. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,164.19 | ▲ 09:30 equity $10,164.19 vs yday $10,164.19 (-0.00) | 09:30 open · cash $10,164.19 · no holdings · equity $10,164.19 vs prior close $10,164.19 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,164.19 | ▲ close $10,164.19 vs 09:30 $10,164.19 (session +0.00) | 16:00 close · cash $10,164.19 · no lots left · equity $10,164.19. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,164.19 | ▲ 09:30 equity $10,164.19 vs yday $10,164.19 (-0.00) | 09:30 open · cash $10,164.19 · no holdings · equity $10,164.19 vs prior close $10,164.19 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,164.19 | ▲ close $10,164.19 vs 09:30 $10,164.19 (session +0.00) | 16:00 close · cash $10,164.19 · no lots left · equity $10,164.19. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,164.19 | ▲ 09:30 equity $10,164.19 vs yday $10,164.19 (-0.00) | 09:30 open · cash $10,164.19 · no holdings · equity $10,164.19 vs prior close $10,164.19 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,164.19 | ▲ close $10,164.19 vs 09:30 $10,164.19 (session +0.00) | 16:00 close · cash $10,164.19 · no lots left · equity $10,164.19. | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,164.19 | ▲ 09:30 equity $10,164.19 vs yday $10,164.19 (-0.00) | 09:30 open · cash $10,164.19 · no holdings · equity $10,164.19 vs prior close $10,164.19 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,164.19 | ▲ close $10,164.19 vs 09:30 $10,164.19 (session +0.00) | 16:00 close · cash $10,164.19 · no lots left · equity $10,164.19. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,164.19 | ▲ 09:30 equity $10,164.19 vs yday $10,164.19 (-0.00) | 09:30 open · cash $10,164.19 · no holdings · equity $10,164.19 vs prior close $10,164.19 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,164.19 | ▲ close $10,164.19 vs 09:30 $10,164.19 (session +0.00) | 16:00 close · cash $10,164.19 · no lots left · equity $10,164.19. | — |
