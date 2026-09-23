# Factor mine action — `overnight_mega_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `overnight_mega` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book

Cash book **-5.76%** ($9,424) · signal-only (no cash/fees) was -14.12%. Starts YES **0/29**. Fills 24 · skips 14 · realized $-576.24.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the same calendar list, kept only when prior-export mcap is at least $50B and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the same calendar list, kept only when prior-export mcap is at least $50B.
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the same calendar list, kept only when prior-export mcap is at least $50B that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
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

- **Universe** `overnight_mega` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,423.78.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `ROST` | 43 | — | $229.55 | +0.00 | $228.99 | -24.08 | -24.08 | +0.00 | -24.08 |
| 2026-08-21 | `ROST` | 43 | $228.99 | $243.85 | +638.98 | — | +0.00 | +638.98 | +614.90 | — |
| 2026-08-21 | `PDD` | 117 | — | $90.03 | +0.00 | $88.38 | -193.05 | -193.05 | +0.00 | -193.05 |
| 2026-08-24 | `PDD` | 117 | $88.38 | $90.95 | +300.69 | — | +0.00 | +300.69 | +107.64 | — |
| 2026-08-25 | `INTU` | 29 | — | $364.35 | +0.00 | $357.46 | -199.81 | -199.81 | +0.00 | -199.81 |
| 2026-08-26 | `INTU` | 29 | $357.46 | $323.47 | -985.71 | — | +0.00 | -985.71 | -1185.52 | — |
| 2026-08-26 | `CM` | 11 | — | $118.50 | +0.00 | $118.20 | -3.30 | -3.30 | +0.00 | -3.30 |
| 2026-08-26 | `CRM` | 6 | — | $199.94 | +0.00 | $205.62 | +34.08 | +34.08 | +0.00 | +34.08 |
| 2026-08-26 | `CRWD` | 7 | — | $182.75 | +0.00 | $189.18 | +45.01 | +45.01 | +0.00 | +45.01 |
| 2026-08-26 | `NVDA` | 6 | — | $212.64 | +0.00 | $209.66 | -17.88 | -17.88 | +0.00 | -17.88 |
| 2026-08-26 | `RY` | 6 | — | $206.95 | +0.00 | $207.21 | +1.56 | +1.56 | +0.00 | +1.56 |
| 2026-08-26 | `SNPS` | 3 | — | $405.10 | +0.00 | $410.00 | +14.70 | +14.70 | +0.00 | +14.70 |
| 2026-08-26 | `TD` | 11 | — | $119.11 | +0.00 | $119.43 | +3.52 | +3.52 | +0.00 | +3.52 |
| 2026-08-27 | `CM` | 11 | $118.20 | $118.77 | +6.27 | — | +0.00 | +6.27 | +2.97 | — |
| 2026-08-27 | `CRM` | 6 | $205.62 | $230.05 | +146.58 | — | +0.00 | +146.58 | +180.66 | — |
| 2026-08-27 | `CRWD` | 7 | $189.18 | $208.25 | +133.49 | — | +0.00 | +133.49 | +178.50 | — |
| 2026-08-27 | `NVDA` | 6 | $209.66 | $222.86 | +79.20 | — | +0.00 | +79.20 | +61.32 | — |
| 2026-08-27 | `RY` | 6 | $207.21 | $206.82 | -2.34 | — | +0.00 | -2.34 | -0.78 | — |
| 2026-08-27 | `SNPS` | 3 | $410.00 | $419.66 | +28.98 | — | +0.00 | +28.98 | +43.68 | — |
| 2026-08-27 | `TD` | 11 | $119.43 | $120.17 | +8.14 | — | +0.00 | +8.14 | +11.66 | — |
| 2026-08-27 | `ADSK` | 19 | — | $261.47 | +0.00 | $270.58 | +173.09 | +173.09 | +0.00 | +173.09 |
| 2026-08-27 | `MRVL` | 19 | — | $253.44 | +0.00 | $241.45 | -227.81 | -227.81 | +0.00 | -227.81 |
| 2026-08-28 | `ADSK` | 19 | $270.58 | $261.16 | -178.98 | — | +0.00 | -178.98 | -5.89 | — |
| 2026-08-28 | `MRVL` | 19 | $241.45 | $225.26 | -307.61 | — | +0.00 | -307.61 | -535.42 | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-08 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-21 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-22 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-23 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | -24.08 | ROST | — | $127.23 | $9,973.80 | ROST×43 |
| 2026-08-21 | +3.25 | $127.23 | ROST×43 | $10,612.78 | +638.98 | -193.05 | PDD | ROST | $74.72 | $10,415.18 | PDD×117 |
| 2026-08-24 | -5.17 | $74.72 | PDD×117 | $10,715.87 | +300.69 | +0.00 | — | PDD | $10,713.42 | $10,713.42 | — |
| 2026-08-25 | +1.80 | $10,713.42 | — | $10,713.42 | +0.00 | -199.81 | INTU | — | $145.19 | $10,511.53 | INTU×29 |
| 2026-08-26 | +2.02 | $145.19 | INTU×29 | $9,525.82 | -985.71 | +77.69 | CM, CRM, CRWD, NVDA, RY, SNPS, TD | INTU | $684.14 | $9,587.27 | CM×11, CRM×6, CRWD×7, NVDA×6, RY×6, SNPS×3, TD×11 |
| 2026-08-27 | — | $684.14 | CM×11, CRM×6, CRWD×7, NVDA×6, RY×6, SNPS×3, TD×11 | $9,987.59 | +400.32 | -54.72 | ADSK, MRVL | CM, CRM, CRWD, NVDA, RY, SNPS, TD | $185.98 | $9,914.55 | ADSK×19, MRVL×19 |
| 2026-08-28 | +0.75 | $185.98 | ADSK×19, MRVL×19 | $9,427.96 | -486.59 | +0.00 | — | ADSK, MRVL | $9,423.78 | $9,423.78 | — |
| 2026-08-31 | -5.85 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-01 | -6.30 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-02 | -3.83 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-03 | -0.90 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-04 | +2.25 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-08 | -11.47 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-09 | -13.95 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-10 | -13.28 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-11 | +0.50 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-14 | -11.00 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-15 | -3.84 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-16 | +5.30 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-17 | +7.38 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-18 | +4.86 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-21 | +12.87 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-22 | -0.50 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |
| 2026-09-23 | +2.29 | $9,423.78 | — | $9,423.78 | -0.00 | +0.00 | — | — | $9,423.78 | $9,423.78 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 43 | $229.55 | $2.12 | — | $127.23 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-5.5; leftover $10000.00 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $127.23 | ▼ close $9,973.80 vs 09:30 $10,000.00 (session -24.08) | 16:00 close · cash $127.23 · equity $9,973.80 vs 09:30 $10,000.00 (-26.20; session marks -24.08) · 1 name(s) marked open→close (per-name table). ROST×43 09:30 $229.55 → close $228.99 -24.08 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.23 | ▲ 09:30 equity $10,612.78 vs yday $9,973.80 (+638.98) | 09:30 open · cash $127.23 (unchanged overnight, no fees) · equity $10,612.78 vs prior close $9,973.80 (+638.98) · 1 name(s) re-marked at the open (per-name table). ROST×43 yday $228.99 → 09:30 $243.85 +638.98 | — |
| 2026-08-21 09:30 ET | **SELL** | `ROST` | 43 | $243.85 | $2.21 | $+610.57 | $10,610.57 | ▲ +610.57 after sell → book $10,610.57; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PDD` | 117 | $90.03 | $2.34 | — | $74.72 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=+6.4; leftover $10610.57 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.72 | ▼ close $10,415.18 vs 09:30 $10,612.78 (session -193.05) | 16:00 close · cash $74.72 · equity $10,415.18 vs 09:30 $10,612.78 (-197.60; session marks -193.05) · 1 name(s) marked open→close (per-name table). PDD×117 09:30 $90.03 → close $88.38 -193.05 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.72 | ▲ 09:30 equity $10,715.87 vs yday $10,415.18 (+300.69) | 09:30 open · cash $74.72 (unchanged overnight, no fees) · equity $10,715.87 vs prior close $10,415.18 (+300.69) · 1 name(s) re-marked at the open (per-name table). PDD×117 yday $88.38 → 09:30 $90.95 +300.69 | — |
| 2026-08-24 09:30 ET | **SELL** | `PDD` | 117 | $90.95 | $2.45 | $+102.85 | $10,713.42 | ▲ +102.85 after sell → book $10,713.42; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,713.42 | ▲ close $10,713.42 vs 09:30 $10,715.87 (session +0.00) | 16:00 close · cash $10,713.42 · no lots left · equity $10,713.42. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,713.42 | ▲ 09:30 equity $10,713.42 vs yday $10,713.42 (+0.00) | 09:30 open · cash $10,713.42 · no holdings · equity $10,713.42 vs prior close $10,713.42 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `INTU` | 29 | $364.35 | $2.08 | — | $145.19 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $10713.42 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.19 | ▼ close $10,511.53 vs 09:30 $10,713.42 (session -199.81) | 16:00 close · cash $145.19 · equity $10,511.53 vs 09:30 $10,713.42 (-201.89; session marks -199.81) · 1 name(s) marked open→close (per-name table). INTU×29 09:30 $364.35 → close $357.46 -199.81 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.19 | ▼ 09:30 equity $9,525.82 vs yday $10,511.53 (-985.71) | 09:30 open · cash $145.19 (unchanged overnight, no fees) · equity $9,525.82 vs prior close $10,511.53 (-985.71) · 1 name(s) re-marked at the open (per-name table). INTU×29 yday $357.46 → 09:30 $323.47 -985.71 | — |
| 2026-08-26 09:30 ET | **SELL** | `INTU` | 29 | $323.47 | $2.16 | $-1189.76 | $9,523.66 | ▼ -1,189.76 after sell → book $9,523.66; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟡 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 11 | $118.50 | $2.02 | — | $8,218.14 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $1360.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CRM` | 6 | $199.94 | $2.01 | — | $7,016.49 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; ret5=+2.1; leftover $1360.52 | join🟡 sector🔴 gen🟢 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 catal🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `CRWD` | 7 | $182.75 | $2.01 | — | $5,735.23 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-12.9; leftover $1360.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NVDA` | 6 | $212.64 | $2.01 | — | $4,457.38 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-3.0; leftover $1360.52 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 catal🟡 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `RY` | 6 | $206.95 | $2.01 | — | $3,213.67 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-2.8; leftover $1360.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SNPS` | 3 | $405.10 | $2.00 | — | $1,996.38 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=+1.1; leftover $1360.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `TD` | 11 | $119.11 | $2.02 | — | $684.14 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=-2.4; leftover $1360.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $684.14 | ▲ close $9,587.27 vs 09:30 $9,525.82 (session +77.69) | 16:00 close · cash $684.14 · equity $9,587.27 vs 09:30 $9,525.82 (+61.45; session marks +77.69) · 7 name(s) marked open→close (per-name table). CM×11 09:30 $118.50 → close $118.20 -3.30; CRM×6 09:30 $199.94 → close $205.62 +34.08; CRWD×7 09:30 $182.75 → close $189.18 +45.01; NVDA×6 09:30 $212.64 → close $209.66 -17.88; RY×6 09:30 $206.95 → close $207.21 +1.56; SNPS×3 09:30 $405.10 → close $410.00 +14.70; TD×11 09:30 $119.11 → close $119.43 +3.52 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $684.14 | ▲ 09:30 equity $9,987.59 vs yday $9,587.27 (+400.32) | 09:30 open · cash $684.14 (unchanged overnight, no fees) · equity $9,987.59 vs prior close $9,587.27 (+400.32) · 7 name(s) re-marked at the open (per-name table). CM×11 yday $118.20 → 09:30 $118.77 +6.27; CRM×6 yday $205.62 → 09:30 $230.05 +146.58; CRWD×7 yday $189.18 → 09:30 $208.25 +133.49; NVDA×6 yday $209.66 → 09:30 $222.86 +79.20; RY×6 yday $207.21 → 09:30 $206.82 -2.34; SNPS×3 yday $410.00 → 09:30 $419.66 +28.98; TD×11 yday $119.43 → 09:30 $120.17 +8.14 | — |
| 2026-08-27 09:30 ET | **SELL** | `CM` | 11 | $118.77 | $2.04 | $-1.10 | $1,988.57 | ▼ -1.10 after sell → book $9,985.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CRM` | 6 | $230.05 | $2.03 | $+176.62 | $3,366.84 | ▲ +176.62 after sell → book $9,983.52; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRWD` | 7 | $208.25 | $2.03 | $+174.46 | $4,822.56 | ▲ +174.46 after sell → book $9,981.49; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NVDA` | 6 | $222.86 | $2.03 | $+57.28 | $6,157.69 | ▲ +57.28 after sell → book $9,979.46; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `RY` | 6 | $206.82 | $2.03 | $-4.82 | $7,396.58 | ▼ -4.82 after sell → book $9,977.43; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `SNPS` | 3 | $419.66 | $2.02 | $+39.66 | $8,653.54 | ▲ +39.66 after sell → book $9,975.41; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TD` | 11 | $120.17 | $2.04 | $+7.59 | $9,973.37 | ▲ +7.59 after sell → book $9,973.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 19 | $261.47 | $2.05 | — | $5,003.39 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $4986.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 19 | $253.44 | $2.05 | — | $185.98 | — | prior-calendar AMC-today / BMO-next, mcap≥$50B; hold 1 sells at next 09:30 so the print gap is in the book; list overnight,overnight_mega,mover_buy; 🔵; ret5=+3.3; leftover $4986.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $185.98 | ▼ close $9,914.55 vs 09:30 $9,987.59 (session -54.72) | 16:00 close · cash $185.98 · equity $9,914.55 vs 09:30 $9,987.59 (-73.04; session marks -54.72) · 2 name(s) marked open→close (per-name table). ADSK×19 09:30 $261.47 → close $270.58 +173.09; MRVL×19 09:30 $253.44 → close $241.45 -227.81 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $185.98 | ▼ 09:30 equity $9,427.96 vs yday $9,914.55 (-486.59) | 09:30 open · cash $185.98 (unchanged overnight, no fees) · equity $9,427.96 vs prior close $9,914.55 (-486.59) · 2 name(s) re-marked at the open (per-name table). ADSK×19 yday $270.58 → 09:30 $261.16 -178.98; MRVL×19 yday $241.45 → 09:30 $225.26 -307.61 | — |
| 2026-08-28 09:30 ET | **SELL** | `ADSK` | 19 | $261.16 | $2.10 | $-10.03 | $5,145.93 | ▼ -10.03 after sell → book $9,425.87; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 19 | $225.26 | $2.09 | $-539.56 | $9,423.78 | ▼ -539.56 after sell → book $9,423.78; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,427.96 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,423.78 | ▲ 09:30 equity $9,423.78 vs yday $9,423.78 (-0.00) | 09:30 open · cash $9,423.78 · no holdings · equity $9,423.78 vs prior close $9,423.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,423.78 | ▲ close $9,423.78 vs 09:30 $9,423.78 (session +0.00) | 16:00 close · cash $9,423.78 · no lots left · equity $9,423.78. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LOW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TGT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TJX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NTES` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WMT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DELL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SNOW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
