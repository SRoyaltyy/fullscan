# Factor mine action — `flatten_vol_g_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · flatten wish-list ∩ vol🟢

Cash book **-10.13%** ($8,987) · signal-only (no cash/fees) was -1.03%. Starts YES **1/18**. Fills 40 · skips 47 · realized $-632.35.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the flatten wish-list (names the flatten board wanted that morning) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the flatten wish-list (names the flatten board wanted that morning).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the volume camera (is this name unusually active?) is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the flatten wish-list (names the flatten board wanted that morning) that pass the must-haves.
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

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $16.50.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `BTBT` | 3333 | — | $1.50 | +0.00 | $1.57 | +233.31 | +233.31 | +0.00 | +233.31 |
| 2026-08-14 | `BETR` | 334 | — | $14.80 | +0.00 | $13.73 | -357.38 | -357.38 | +0.00 | -357.38 |
| 2026-08-17 | `BTBT` | 3333 | $1.57 | $1.52 | -166.65 | $1.60 | +266.64 | +99.99 | +66.66 | +333.30 |
| 2026-08-17 | `BETR` | 334 | $13.73 | $13.67 | -20.04 | $13.54 | -43.42 | -63.46 | -377.42 | -420.84 |
| 2026-08-17 | `TMC` | 2 | — | $4.05 | +0.00 | $3.77 | -0.56 | -0.56 | +0.00 | -0.56 |
| 2026-08-18 | `BTBT` | 3333 | $1.60 | $1.54 | -199.98 | $1.45 | -299.97 | -499.95 | +133.32 | -166.65 |
| 2026-08-18 | `BETR` | 334 | $13.54 | $13.21 | -110.22 | $13.05 | -53.44 | -163.66 | -531.06 | -584.50 |
| 2026-08-18 | `TMC` | 2 | $3.77 | $3.72 | -0.10 | $3.92 | +0.40 | +0.30 | -0.66 | -0.26 |
| 2026-08-19 | `BTBT` | 3333 | $1.45 | $1.42 | -99.99 | — | +0.00 | -99.99 | -266.64 | — |
| 2026-08-19 | `BETR` | 334 | $13.05 | $13.03 | -6.68 | — | +0.00 | -6.68 | -591.18 | — |
| 2026-08-19 | `TMC` | 2 | $3.92 | $3.93 | +0.02 | $3.97 | +0.08 | +0.10 | -0.24 | -0.16 |
| 2026-08-20 | `TMC` | 2 | $3.97 | $3.92 | -0.10 | — | +0.00 | -0.10 | -0.26 | — |
| 2026-08-20 | `AG` | 55 | — | $20.55 | +0.00 | $21.19 | +35.20 | +35.20 | +0.00 | +35.20 |
| 2026-08-20 | `BHP` | 12 | — | $91.01 | +0.00 | $93.63 | +31.44 | +31.44 | +0.00 | +31.44 |
| 2026-08-20 | `CDE` | 54 | — | $20.65 | +0.00 | $21.11 | +24.84 | +24.84 | +0.00 | +24.84 |
| 2026-08-20 | `HDSN` | 195 | — | $5.77 | +0.00 | $5.57 | -39.00 | -39.00 | +0.00 | -39.00 |
| 2026-08-20 | `IAG` | 57 | — | $19.63 | +0.00 | $20.50 | +49.59 | +49.59 | +0.00 | +49.59 |
| 2026-08-20 | `KGC` | 38 | — | $29.63 | +0.00 | $31.43 | +68.40 | +68.40 | +0.00 | +68.40 |
| 2026-08-20 | `NFGC` | 646 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 7 | — | $144.54 | +0.00 | $150.25 | +39.97 | +39.97 | +0.00 | +39.97 |
| 2026-08-21 | `AG` | 55 | $21.19 | $21.90 | +39.05 | $21.09 | -44.55 | -5.50 | +74.25 | +29.70 |
| 2026-08-21 | `BHP` | 12 | $93.63 | $95.72 | +25.08 | $97.03 | +15.72 | +40.80 | +56.52 | +72.24 |
| 2026-08-21 | `CDE` | 54 | $21.11 | $21.75 | +34.56 | $20.97 | -42.12 | -7.56 | +59.40 | +17.28 |
| 2026-08-21 | `HDSN` | 195 | $5.57 | $5.67 | +19.50 | $5.63 | -7.80 | +11.70 | -19.50 | -27.30 |
| 2026-08-21 | `IAG` | 57 | $20.50 | $21.17 | +38.19 | $21.14 | -1.71 | +36.48 | +87.78 | +86.07 |
| 2026-08-21 | `KGC` | 38 | $31.43 | $32.17 | +28.12 | $32.76 | +22.42 | +50.54 | +96.52 | +118.94 |
| 2026-08-21 | `NFGC` | 646 | $1.75 | $1.79 | +25.84 | $1.84 | +32.30 | +58.14 | +25.84 | +58.14 |
| 2026-08-21 | `WPM` | 7 | $150.25 | $154.70 | +31.15 | $157.78 | +21.56 | +52.71 | +71.12 | +92.68 |
| 2026-08-21 | `AUPH` | 1 | — | $17.20 | +0.00 | $16.65 | -0.55 | -0.55 | +0.00 | -0.55 |
| 2026-08-21 | `ARCT` | 1 | — | $11.13 | +0.00 | $13.45 | +2.32 | +2.32 | +0.00 | +2.32 |
| 2026-08-21 | `AUTL` | 8 | — | $2.47 | +0.00 | $2.41 | -0.48 | -0.48 | +0.00 | -0.48 |
| 2026-08-21 | `CRDL` | 11 | — | $1.93 | +0.00 | $1.86 | -0.77 | -0.77 | +0.00 | -0.77 |
| 2026-08-21 | `CYPH` | 16 | — | $1.32 | +0.00 | $1.42 | +1.60 | +1.60 | +0.00 | +1.60 |
| 2026-08-24 | `AG` | 55 | $21.09 | $21.30 | +11.55 | $20.83 | -25.85 | -14.30 | +41.25 | +15.40 |
| 2026-08-24 | `BHP` | 12 | $97.03 | $97.31 | +3.36 | $97.13 | -2.16 | +1.20 | +75.60 | +73.44 |
| 2026-08-24 | `CDE` | 54 | $20.97 | $21.26 | +15.66 | $20.88 | -20.52 | -4.86 | +32.94 | +12.42 |
| 2026-08-24 | `HDSN` | 195 | $5.63 | $5.69 | +11.70 | $5.52 | -33.15 | -21.45 | -15.60 | -48.75 |
| 2026-08-24 | `IAG` | 57 | $21.14 | $21.38 | +13.68 | $21.80 | +23.94 | +37.62 | +99.75 | +123.69 |
| 2026-08-24 | `KGC` | 38 | $32.76 | $33.03 | +10.26 | $32.98 | -1.90 | +8.36 | +129.20 | +127.30 |
| 2026-08-24 | `NFGC` | 646 | $1.84 | $1.86 | +12.92 | $1.90 | +25.84 | +38.76 | +71.06 | +96.90 |
| 2026-08-24 | `WPM` | 7 | $157.78 | $159.50 | +12.04 | $160.19 | +4.83 | +16.87 | +104.72 | +109.55 |
| 2026-08-24 | `AUPH` | 1 | $16.65 | $16.57 | -0.08 | $16.57 | +0.00 | -0.08 | -0.63 | -0.63 |
| 2026-08-24 | `ARCT` | 1 | $13.45 | $13.33 | -0.12 | $14.34 | +1.01 | +0.89 | +2.20 | +3.21 |
| 2026-08-24 | `AUTL` | 8 | $2.41 | $2.40 | -0.08 | $2.34 | -0.48 | -0.56 | -0.56 | -1.04 |
| 2026-08-24 | `CRDL` | 11 | $1.86 | $1.88 | +0.22 | $1.86 | -0.22 | +0.00 | -0.55 | -0.77 |
| 2026-08-24 | `CYPH` | 16 | $1.42 | $1.83 | +6.56 | $1.68 | -2.40 | +4.16 | +8.16 | +5.76 |
| 2026-08-25 | `AG` | 55 | $20.83 | $20.32 | -28.05 | — | +0.00 | -28.05 | -12.65 | — |
| 2026-08-25 | `BHP` | 12 | $97.13 | $95.86 | -15.24 | — | +0.00 | -15.24 | +58.20 | — |
| 2026-08-25 | `CDE` | 54 | $20.88 | $20.47 | -22.14 | — | +0.00 | -22.14 | -9.72 | — |
| 2026-08-25 | `HDSN` | 195 | $5.52 | $5.53 | +1.95 | — | +0.00 | +1.95 | -46.80 | — |
| 2026-08-25 | `IAG` | 57 | $21.80 | $21.21 | -33.63 | — | +0.00 | -33.63 | +90.06 | — |
| 2026-08-25 | `KGC` | 38 | $32.98 | $32.32 | -25.08 | — | +0.00 | -25.08 | +102.22 | — |
| 2026-08-25 | `NFGC` | 646 | $1.90 | $1.90 | +0.00 | — | +0.00 | +0.00 | +96.90 | — |
| 2026-08-25 | `WPM` | 7 | $160.19 | $156.51 | -25.76 | — | +0.00 | -25.76 | +83.79 | — |
| 2026-08-25 | `AUPH` | 1 | $16.57 | $16.63 | +0.06 | $16.75 | +0.12 | +0.18 | -0.57 | -0.45 |
| 2026-08-25 | `ARCT` | 1 | $14.34 | $14.12 | -0.22 | $15.44 | +1.32 | +1.10 | +2.99 | +4.31 |
| 2026-08-25 | `AUTL` | 8 | $2.34 | $2.38 | +0.32 | $2.44 | +0.48 | +0.80 | -0.72 | -0.24 |
| 2026-08-25 | `CRDL` | 11 | $1.86 | $1.89 | +0.33 | $2.00 | +1.21 | +1.54 | -0.44 | +0.77 |
| 2026-08-25 | `CYPH` | 16 | $1.68 | $1.56 | -1.92 | $1.64 | +1.28 | -0.64 | +3.84 | +5.12 |
| 2026-08-26 | `AUPH` | 1 | $16.75 | $16.60 | -0.15 | — | +0.00 | -0.15 | -0.60 | — |
| 2026-08-26 | `ARCT` | 1 | $15.44 | $15.35 | -0.09 | — | +0.00 | -0.09 | +4.22 | — |
| 2026-08-26 | `AUTL` | 8 | $2.44 | $2.41 | -0.24 | — | +0.00 | -0.24 | -0.48 | — |
| 2026-08-26 | `CRDL` | 11 | $2.00 | $2.03 | +0.33 | — | +0.00 | +0.33 | +1.10 | — |
| 2026-08-26 | `CYPH` | 16 | $1.64 | $1.60 | -0.64 | — | +0.00 | -0.64 | +4.48 | — |
| 2026-08-27 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-28 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-31 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `RVTY` | 70 | — | $132.45 | +0.00 | $130.63 | -127.40 | -127.40 | +0.00 | -127.40 |
| 2026-09-04 | `RVTY` | 70 | $130.63 | $130.03 | -42.00 | $130.22 | +13.30 | -28.70 | -169.40 | -156.10 |
| 2026-09-04 | `CABA` | 3 | — | $3.46 | +0.00 | $3.47 | +0.03 | +0.03 | +0.00 | +0.03 |
| 2026-09-04 | `ALEC` | 5 | — | $2.52 | +0.00 | $2.46 | -0.30 | -0.30 | +0.00 | -0.30 |
| 2026-09-04 | `BHC` | 1 | — | $6.71 | +0.00 | $6.56 | -0.15 | -0.15 | +0.00 | -0.15 |
| 2026-09-04 | `BMEA` | 7 | — | $1.90 | +0.00 | $2.03 | +0.91 | +0.91 | +0.00 | +0.91 |
| 2026-09-04 | `OABI` | 2 | — | $4.78 | +0.00 | $4.33 | -0.90 | -0.90 | +0.00 | -0.90 |
| 2026-09-04 | `OPK` | 8 | — | $1.59 | +0.00 | $1.64 | +0.40 | +0.40 | +0.00 | +0.40 |
| 2026-09-04 | `VIR` | 1 | — | $11.31 | +0.00 | $11.38 | +0.07 | +0.07 | +0.00 | +0.07 |
| 2026-09-08 | `RVTY` | 70 | $130.22 | $128.50 | -120.40 | $127.08 | -99.40 | -219.80 | -276.50 | -375.90 |
| 2026-09-08 | `CABA` | 3 | $3.47 | $3.43 | -0.12 | $3.27 | -0.48 | -0.60 | -0.09 | -0.57 |
| 2026-09-08 | `ALEC` | 5 | $2.46 | $2.38 | -0.40 | $2.47 | +0.45 | +0.05 | -0.70 | -0.25 |
| 2026-09-08 | `BHC` | 1 | $6.56 | $6.57 | +0.01 | $6.43 | -0.14 | -0.13 | -0.14 | -0.28 |
| 2026-09-08 | `BMEA` | 7 | $2.03 | $2.00 | -0.21 | $1.93 | -0.49 | -0.70 | +0.70 | +0.21 |
| 2026-09-08 | `OABI` | 2 | $4.33 | $4.30 | -0.06 | $4.24 | -0.12 | -0.18 | -0.96 | -1.08 |
| 2026-09-08 | `OPK` | 8 | $1.64 | $1.63 | -0.08 | $1.59 | -0.32 | -0.40 | +0.32 | +0.00 |
| 2026-09-08 | `VIR` | 1 | $11.38 | $11.22 | -0.16 | $11.18 | -0.04 | -0.20 | -0.09 | -0.13 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -124.07 | BTBT, BETR | — | $10.00 | $9,828.63 | BTBT×3333, BETR×334 |
| 2026-08-17 | +2.25 | $10.00 | BTBT×3333, BETR×334 | $9,641.94 | -186.69 | +222.66 | TMC | — | $1.81 | $9,864.51 | BTBT×3333, BETR×334, TMC×2 |
| 2026-08-18 | -6.20 | $1.81 | BTBT×3333, BETR×334, TMC×2 | $9,554.21 | -310.30 | -353.01 | — | — | $1.81 | $9,201.20 | BTBT×3333, BETR×334, TMC×2 |
| 2026-08-19 | -7.20 | $1.81 | BTBT×3333, BETR×334, TMC×2 | $9,094.55 | -106.65 | +0.08 | — | BTBT, BETR | $9,038.70 | $9,046.64 | TMC×2 |
| 2026-08-20 | +1.12 | $9,038.70 | TMC×2 | $9,046.54 | -0.10 | +210.44 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | TMC | $173.17 | $9,233.36 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7 |
| 2026-08-21 | +3.25 | $173.17 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7 | $9,474.85 | +241.49 | -2.06 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $81.72 | $9,471.78 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 |
| 2026-08-24 | -5.17 | $81.72 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | $9,569.45 | +97.67 | -31.06 | — | — | $81.72 | $9,538.39 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 |
| 2026-08-25 | +1.80 | $81.72 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | $9,389.01 | -149.38 | +4.41 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $9,269.67 | $9,369.62 | AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 |
| 2026-08-26 | +2.02 | $9,269.67 | AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | $9,368.83 | -0.79 | +0.00 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $9,367.63 | $9,367.63 | — |
| 2026-08-27 | — | $9,367.63 | — | $9,367.63 | -0.00 | +0.00 | — | — | $9,367.63 | $9,367.63 | — |
| 2026-08-28 | +0.75 | $9,367.63 | — | $9,367.63 | -0.00 | +0.00 | — | — | $9,367.63 | $9,367.63 | — |
| 2026-08-31 | -5.85 | $9,367.63 | — | $9,367.63 | -0.00 | +0.00 | — | — | $9,367.63 | $9,367.63 | — |
| 2026-09-01 | -6.30 | $9,367.63 | — | $9,367.63 | -0.00 | +0.00 | — | — | $9,367.63 | $9,367.63 | — |
| 2026-09-02 | -3.83 | $9,367.63 | — | $9,367.63 | -0.00 | +0.00 | — | — | $9,367.63 | $9,367.63 | — |
| 2026-09-03 | -0.90 | $9,367.63 | — | $9,367.63 | -0.00 | -127.40 | RVTY | — | $93.93 | $9,238.03 | RVTY×70 |
| 2026-09-04 | +2.25 | $93.93 | RVTY×70 | $9,196.03 | -42.00 | +13.36 | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR | — | $16.50 | $9,208.54 | RVTY×70, CABA×3, ALEC×5, BHC×1, BMEA×7, OABI×2, OPK×8, VIR×1 |
| 2026-09-08 | -11.47 | $16.50 | RVTY×70, CABA×3, ALEC×5, BHC×1, BMEA×7, OABI×2, OPK×8, VIR×1 | $9,087.12 | -121.42 | -100.54 | — | — | $16.50 | $8,986.58 | RVTY×70, CABA×3, ALEC×5, BHC×1, BMEA×7, OABI×2, OPK×8, VIR×1 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3333 | $1.50 | $43.00 | — | $4,957.50 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 334 | $14.80 | $4.31 | — | $10.00 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-9.9; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.00 | ▼ close $9,828.63 vs 09:30 $10,000.00 (session -124.07) | 16:00 close · cash $10.00 · equity $9,828.63 vs 09:30 $10,000.00 (-171.37; session marks -124.07) · 2 name(s) marked open→close (per-name table). BTBT×3333 09:30 $1.50 → close $1.57 +233.31; BETR×334 09:30 $14.80 → close $13.73 -357.38 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.00 | ▼ 09:30 equity $9,641.94 vs yday $9,828.63 (-186.69) | 09:30 open · cash $10.00 (unchanged overnight, no fees) · equity $9,641.94 vs prior close $9,828.63 (-186.69) · 2 name(s) re-marked at the open (per-name table). BTBT×3333 yday $1.57 → 09:30 $1.52 -166.65; BETR×334 yday $13.73 → 09:30 $13.67 -20.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $1.81 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $10.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.81 | ▲ close $9,864.51 vs 09:30 $9,641.94 (session +222.66) | 16:00 close · cash $1.81 · equity $9,864.51 vs 09:30 $9,641.94 (+222.57; session marks +222.66) · 3 name(s) marked open→close (per-name table). BTBT×3333 09:30 $1.52 → close $1.60 +266.64; BETR×334 09:30 $13.67 → close $13.54 -43.42; TMC×2 09:30 $4.05 → close $3.77 -0.56 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.81 | ▼ 09:30 equity $9,554.21 vs yday $9,864.51 (-310.30) | 09:30 open · cash $1.81 (unchanged overnight, no fees) · equity $9,554.21 vs prior close $9,864.51 (-310.30) · 3 name(s) re-marked at the open (per-name table). BTBT×3333 yday $1.60 → 09:30 $1.54 -199.98; BETR×334 yday $13.54 → 09:30 $13.21 -110.22; TMC×2 yday $3.77 → 09:30 $3.72 -0.10 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.81 | ▼ close $9,201.20 vs 09:30 $9,554.21 (session -353.01) | 16:00 close · cash $1.81 · equity $9,201.20 vs 09:30 $9,554.21 (-353.01; session marks -353.01) · 3 name(s) marked open→close (per-name table). BTBT×3333 09:30 $1.54 → close $1.45 -299.97; BETR×334 09:30 $13.21 → close $13.05 -53.44; TMC×2 09:30 $3.72 → close $3.92 +0.40 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.81 | ▼ 09:30 equity $9,094.55 vs yday $9,201.20 (-106.65) | 09:30 open · cash $1.81 (unchanged overnight, no fees) · equity $9,094.55 vs prior close $9,201.20 (-106.65) · 3 name(s) re-marked at the open (per-name table). BTBT×3333 yday $1.45 → 09:30 $1.42 -99.99; BETR×334 yday $13.05 → 09:30 $13.03 -6.68; TMC×2 yday $3.92 → 09:30 $3.93 +0.02 | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 3333 | $1.42 | $43.59 | $-353.22 | $4,691.08 | ▼ -353.22 after sell → book $9,050.96; vs 09:30 mark -43.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 334 | $13.03 | $4.40 | $-599.89 | $9,038.70 | ▼ -599.89 after sell → book $9,046.56; vs 09:30 mark -4.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,038.70 | ▲ close $9,046.64 vs 09:30 $9,094.55 (session +0.08) | 16:00 close · cash $9,038.70 · equity $9,046.64 vs 09:30 $9,094.55 (-47.91; session marks +0.08) · 1 name(s) marked open→close (per-name table). TMC×2 09:30 $3.93 → close $3.97 +0.08 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,038.70 | ▼ 09:30 equity $9,046.54 vs yday $9,046.64 (-0.10) | 09:30 open · cash $9,038.70 (unchanged overnight, no fees) · equity $9,046.54 vs prior close $9,046.64 (-0.10) · 1 name(s) re-marked at the open (per-name table). TMC×2 yday $3.97 → 09:30 $3.92 -0.10 | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 2 | $3.92 | $0.10 | $-0.45 | $9,046.44 | ▼ -0.45 after sell → book $9,046.44; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 55 | $20.55 | $2.15 | — | $7,914.03 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $6,819.89 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 54 | $20.65 | $2.15 | — | $5,702.64 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 195 | $5.77 | $2.58 | — | $4,574.91 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 57 | $19.63 | $2.16 | — | $3,453.84 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 38 | $29.63 | $2.10 | — | $2,325.80 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 646 | $1.75 | $8.33 | — | $1,186.96 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $173.17 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.17 | ▲ close $9,233.36 vs 09:30 $9,046.54 (session +210.44) | 16:00 close · cash $173.17 · equity $9,233.36 vs 09:30 $9,046.54 (+186.82; session marks +210.44) · 8 name(s) marked open→close (per-name table). AG×55 09:30 $20.55 → close $21.19 +35.20; BHP×12 09:30 $91.01 → close $93.63 +31.44; CDE×54 09:30 $20.65 → close $21.11 +24.84; HDSN×195 09:30 $5.77 → close $5.57 -39.00; IAG×57 09:30 $19.63 → close $20.50 +49.59; KGC×38 09:30 $29.63 → close $31.43 +68.40; NFGC×646 09:30 $1.75 → close $1.75 +0.00; WPM×7 09:30 $144.54 → close $150.25 +39.97 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.17 | ▲ 09:30 equity $9,474.85 vs yday $9,233.36 (+241.49) | 09:30 open · cash $173.17 (unchanged overnight, no fees) · equity $9,474.85 vs prior close $9,233.36 (+241.49) · 8 name(s) re-marked at the open (per-name table). AG×55 yday $21.19 → 09:30 $21.90 +39.05; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×54 yday $21.11 → 09:30 $21.75 +34.56; HDSN×195 yday $5.57 → 09:30 $5.67 +19.50; IAG×57 yday $20.50 → 09:30 $21.17 +38.19; KGC×38 yday $31.43 → 09:30 $32.17 +28.12; NFGC×646 yday $1.75 → 09:30 $1.79 +25.84; WPM×7 yday $150.25 → 09:30 $154.70 +31.15 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $155.80 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $144.55 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 8 | $2.47 | $0.22 | — | $124.57 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 11 | $1.93 | $0.25 | — | $103.09 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 16 | $1.32 | $0.26 | — | $81.72 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.72 | ▼ close $9,471.78 vs 09:30 $9,474.85 (session -2.06) | 16:00 close · cash $81.72 · equity $9,471.78 vs 09:30 $9,474.85 (-3.07; session marks -2.06) · 13 name(s) marked open→close (per-name table). AG×55 09:30 $21.90 → close $21.09 -44.55; BHP×12 09:30 $95.72 → close $97.03 +15.72; CDE×54 09:30 $21.75 → close $20.97 -42.12; HDSN×195 09:30 $5.67 → close $5.63 -7.80; IAG×57 09:30 $21.17 → close $21.14 -1.71; KGC×38 09:30 $32.17 → close $32.76 +22.42; NFGC×646 09:30 $1.79 → close $1.84 +32.30; WPM×7 09:30 $154.70 → close $157.78 +21.56; AUPH×1 09:30 $17.20 → close $16.65 -0.55; ARCT×1 09:30 $11.13 → close $13.45 +2.32; AUTL×8 09:30 $2.47 → close $2.41 -0.48; CRDL×11 09:30 $1.93 → close $1.86 -0.77; CYPH×16 09:30 $1.32 → close $1.42 +1.60 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.72 | ▲ 09:30 equity $9,569.45 vs yday $9,471.78 (+97.67) | 09:30 open · cash $81.72 (unchanged overnight, no fees) · equity $9,569.45 vs prior close $9,471.78 (+97.67) · 13 name(s) re-marked at the open (per-name table). AG×55 yday $21.09 → 09:30 $21.30 +11.55; BHP×12 yday $97.03 → 09:30 $97.31 +3.36; CDE×54 yday $20.97 → 09:30 $21.26 +15.66; HDSN×195 yday $5.63 → 09:30 $5.69 +11.70; IAG×57 yday $21.14 → 09:30 $21.38 +13.68; KGC×38 yday $32.76 → 09:30 $33.03 +10.26; NFGC×646 yday $1.84 → 09:30 $1.86 +12.92; WPM×7 yday $157.78 → 09:30 $159.50 +12.04; AUPH×1 yday $16.65 → 09:30 $16.57 -0.08; ARCT×1 yday $13.45 → 09:30 $13.33 -0.12; AUTL×8 yday $2.41 → 09:30 $2.40 -0.08; CRDL×11 yday $1.86 → 09:30 $1.88 +0.22; CYPH×16 yday $1.42 → 09:30 $1.83 +6.56 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.72 | ▼ close $9,538.39 vs 09:30 $9,569.45 (session -31.06) | 16:00 close · cash $81.72 · equity $9,538.39 vs 09:30 $9,569.45 (-31.06; session marks -31.06) · 13 name(s) marked open→close (per-name table). AG×55 09:30 $21.30 → close $20.83 -25.85; BHP×12 09:30 $97.31 → close $97.13 -2.16; CDE×54 09:30 $21.26 → close $20.88 -20.52; HDSN×195 09:30 $5.69 → close $5.52 -33.15; IAG×57 09:30 $21.38 → close $21.80 +23.94; KGC×38 09:30 $33.03 → close $32.98 -1.90; NFGC×646 09:30 $1.86 → close $1.90 +25.84; WPM×7 09:30 $159.50 → close $160.19 +4.83; AUPH×1 09:30 $16.57 → close $16.57 +0.00; ARCT×1 09:30 $13.33 → close $14.34 +1.01; AUTL×8 09:30 $2.40 → close $2.34 -0.48; CRDL×11 09:30 $1.88 → close $1.86 -0.22; CYPH×16 09:30 $1.83 → close $1.68 -2.40 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.72 | ▼ 09:30 equity $9,389.01 vs yday $9,538.39 (-149.38) | 09:30 open · cash $81.72 (unchanged overnight, no fees) · equity $9,389.01 vs prior close $9,538.39 (-149.38) · 13 name(s) re-marked at the open (per-name table). AG×55 yday $20.83 → 09:30 $20.32 -28.05; BHP×12 yday $97.13 → 09:30 $95.86 -15.24; CDE×54 yday $20.88 → 09:30 $20.47 -22.14; HDSN×195 yday $5.52 → 09:30 $5.53 +1.95; IAG×57 yday $21.80 → 09:30 $21.21 -33.63; KGC×38 yday $32.98 → 09:30 $32.32 -25.08; NFGC×646 yday $1.90 → 09:30 $1.90 +0.00; WPM×7 yday $160.19 → 09:30 $156.51 -25.76; AUPH×1 yday $16.57 → 09:30 $16.63 +0.06; ARCT×1 yday $14.34 → 09:30 $14.12 -0.22; AUTL×8 yday $2.34 → 09:30 $2.38 +0.32; CRDL×11 yday $1.86 → 09:30 $1.89 +0.33; CYPH×16 yday $1.68 → 09:30 $1.56 -1.92 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 55 | $20.32 | $2.17 | $-16.98 | $1,197.14 | ▼ -16.98 after sell → book $9,386.83; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 12 | $95.86 | $2.05 | $+54.13 | $2,345.41 | ▲ +54.13 after sell → book $9,384.78; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 54 | $20.47 | $2.17 | $-14.04 | $3,448.62 | ▼ -14.04 after sell → book $9,382.61; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 195 | $5.53 | $2.62 | $-51.99 | $4,524.36 | ▼ -51.99 after sell → book $9,380.00; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 57 | $21.21 | $2.18 | $+85.72 | $5,731.14 | ▲ +85.72 after sell → book $9,377.81; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 38 | $32.32 | $2.12 | $+97.99 | $6,957.18 | ▲ +97.99 after sell → book $9,375.69; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 646 | $1.90 | $8.45 | $+80.12 | $8,176.13 | ▲ +80.12 after sell → book $9,367.24; vs 09:30 mark -8.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 7 | $156.51 | $2.03 | $+79.75 | $9,269.67 | ▲ +79.75 after sell → book $9,365.21; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,269.67 | ▲ close $9,369.62 vs 09:30 $9,389.01 (session +4.41) | 16:00 close · cash $9,269.67 · equity $9,369.62 vs 09:30 $9,389.01 (-19.39; session marks +4.41) · 5 name(s) marked open→close (per-name table). AUPH×1 09:30 $16.63 → close $16.75 +0.12; ARCT×1 09:30 $14.12 → close $15.44 +1.32; AUTL×8 09:30 $2.38 → close $2.44 +0.48; CRDL×11 09:30 $1.89 → close $2.00 +1.21; CYPH×16 09:30 $1.56 → close $1.64 +1.28 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,269.67 | ▼ 09:30 equity $9,368.83 vs yday $9,369.62 (-0.79) | 09:30 open · cash $9,269.67 (unchanged overnight, no fees) · equity $9,368.83 vs prior close $9,369.62 (-0.79) · 5 name(s) re-marked at the open (per-name table). AUPH×1 yday $16.75 → 09:30 $16.60 -0.15; ARCT×1 yday $15.44 → 09:30 $15.35 -0.09; AUTL×8 yday $2.44 → 09:30 $2.41 -0.24; CRDL×11 yday $2.00 → 09:30 $2.03 +0.33; CYPH×16 yday $1.64 → 09:30 $1.60 -0.64 | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $9,286.08 | ▼ -0.96 after sell → book $9,368.64; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $9,301.25 | ▲ +3.93 after sell → book $9,368.46; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 8 | $2.41 | $0.24 | $-0.94 | $9,320.30 | ▼ -0.94 after sell → book $9,368.23; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 11 | $2.03 | $0.28 | $+0.58 | $9,342.35 | ▲ +0.58 after sell → book $9,367.95; vs 09:30 mark -0.28 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 16 | $1.60 | $0.32 | $+3.90 | $9,367.63 | ▲ +3.90 after sell → book $9,367.63; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,367.63 | ▲ close $9,367.63 vs 09:30 $9,368.83 (session +0.00) | 16:00 close · cash $9,367.63 · no lots left · equity $9,367.63. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,367.63 | ▲ 09:30 equity $9,367.63 vs yday $9,367.63 (-0.00) | 09:30 open · cash $9,367.63 · no holdings · equity $9,367.63 vs prior close $9,367.63 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,367.63 | ▲ close $9,367.63 vs 09:30 $9,367.63 (session +0.00) | 16:00 close · cash $9,367.63 · no lots left · equity $9,367.63. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,367.63 | ▲ 09:30 equity $9,367.63 vs yday $9,367.63 (-0.00) | 09:30 open · cash $9,367.63 · no holdings · equity $9,367.63 vs prior close $9,367.63 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,367.63 | ▲ close $9,367.63 vs 09:30 $9,367.63 (session +0.00) | 16:00 close · cash $9,367.63 · no lots left · equity $9,367.63. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,367.63 | ▲ 09:30 equity $9,367.63 vs yday $9,367.63 (-0.00) | 09:30 open · cash $9,367.63 · no holdings · equity $9,367.63 vs prior close $9,367.63 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,367.63 | ▲ close $9,367.63 vs 09:30 $9,367.63 (session +0.00) | 16:00 close · cash $9,367.63 · no lots left · equity $9,367.63. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,367.63 | ▲ 09:30 equity $9,367.63 vs yday $9,367.63 (-0.00) | 09:30 open · cash $9,367.63 · no holdings · equity $9,367.63 vs prior close $9,367.63 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,367.63 | ▲ close $9,367.63 vs 09:30 $9,367.63 (session +0.00) | 16:00 close · cash $9,367.63 · no lots left · equity $9,367.63. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,367.63 | ▲ 09:30 equity $9,367.63 vs yday $9,367.63 (-0.00) | 09:30 open · cash $9,367.63 · no holdings · equity $9,367.63 vs prior close $9,367.63 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,367.63 | ▲ close $9,367.63 vs 09:30 $9,367.63 (session +0.00) | 16:00 close · cash $9,367.63 · no lots left · equity $9,367.63. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,367.63 | ▲ 09:30 equity $9,367.63 vs yday $9,367.63 (-0.00) | 09:30 open · cash $9,367.63 · no holdings · equity $9,367.63 vs prior close $9,367.63 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 70 | $132.45 | $2.20 | — | $93.93 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $9367.63 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.93 | ▼ close $9,238.03 vs 09:30 $9,367.63 (session -127.40) | 16:00 close · cash $93.93 · equity $9,238.03 vs 09:30 $9,367.63 (-129.60; session marks -127.40) · 1 name(s) marked open→close (per-name table). RVTY×70 09:30 $132.45 → close $130.63 -127.40 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.93 | ▼ 09:30 equity $9,196.03 vs yday $9,238.03 (-42.00) | 09:30 open · cash $93.93 (unchanged overnight, no fees) · equity $9,196.03 vs prior close $9,238.03 (-42.00) · 1 name(s) re-marked at the open (per-name table). RVTY×70 yday $130.63 → 09:30 $130.03 -42.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 3 | $3.46 | $0.11 | — | $83.43 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 5 | $2.52 | $0.14 | — | $70.69 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 1 | $6.71 | $0.07 | — | $63.91 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 7 | $1.90 | $0.15 | — | $50.46 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 2 | $4.78 | $0.10 | — | $40.80 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 8 | $1.59 | $0.15 | — | $27.93 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $13.42 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $16.50 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.50 | ▲ close $9,208.54 vs 09:30 $9,196.03 (session +13.36) | 16:00 close · cash $16.50 · equity $9,208.54 vs 09:30 $9,196.03 (+12.51; session marks +13.36) · 8 name(s) marked open→close (per-name table). RVTY×70 09:30 $130.03 → close $130.22 +13.30; CABA×3 09:30 $3.46 → close $3.47 +0.03; ALEC×5 09:30 $2.52 → close $2.46 -0.30; BHC×1 09:30 $6.71 → close $6.56 -0.15; BMEA×7 09:30 $1.90 → close $2.03 +0.91; OABI×2 09:30 $4.78 → close $4.33 -0.90; OPK×8 09:30 $1.59 → close $1.64 +0.40; VIR×1 09:30 $11.31 → close $11.38 +0.07 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.50 | ▼ 09:30 equity $9,087.12 vs yday $9,208.54 (-121.42) | 09:30 open · cash $16.50 (unchanged overnight, no fees) · equity $9,087.12 vs prior close $9,208.54 (-121.42) · 8 name(s) re-marked at the open (per-name table). RVTY×70 yday $130.22 → 09:30 $128.50 -120.40; CABA×3 yday $3.47 → 09:30 $3.43 -0.12; ALEC×5 yday $2.46 → 09:30 $2.38 -0.40; BHC×1 yday $6.56 → 09:30 $6.57 +0.01; BMEA×7 yday $2.03 → 09:30 $2.00 -0.21; OABI×2 yday $4.33 → 09:30 $4.30 -0.06; OPK×8 yday $1.64 → 09:30 $1.63 -0.08; VIR×1 yday $11.38 → 09:30 $11.22 -0.16 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.50 | ▼ close $8,986.58 vs 09:30 $9,087.12 (session -100.54) | 16:00 close · cash $16.50 · equity $8,986.58 vs 09:30 $9,087.12 (-100.54; session marks -100.54) · 8 name(s) marked open→close (per-name table). RVTY×70 09:30 $128.50 → close $127.08 -99.40; CABA×3 09:30 $3.43 → close $3.27 -0.48; ALEC×5 09:30 $2.38 → close $2.47 +0.45; BHC×1 09:30 $6.57 → close $6.43 -0.14; BMEA×7 09:30 $2.00 → close $1.93 -0.49; OABI×2 09:30 $4.30 → close $4.24 -0.12; OPK×8 09:30 $1.63 → close $1.59 -0.32; VIR×1 09:30 $11.22 → close $11.18 -0.04 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 21.65 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 21.65 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 21.65 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RVTY` | 70 | 2026-09-03 @ $132.45 | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+3.6; leftover $9367.63 |
| `CABA` | 3 | 2026-09-04 @ $3.46 | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $13.42 |
| `ALEC` | 5 | 2026-09-04 @ $2.52 | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+5.0; leftover $13.42 |
| `BHC` | 1 | 2026-09-04 @ $6.71 | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $13.42 |
| `BMEA` | 7 | 2026-09-04 @ $1.90 | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.7; leftover $13.42 |
| `OABI` | 2 | 2026-09-04 @ $4.78 | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+3.0; leftover $13.42 |
| `OPK` | 8 | 2026-09-04 @ $1.59 | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.3; leftover $13.42 |
| `VIR` | 1 | 2026-09-04 @ $11.31 | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+1.2; leftover $13.42 |
