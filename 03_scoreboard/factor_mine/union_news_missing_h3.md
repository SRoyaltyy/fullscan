# Factor mine action — `union_news_missing_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_missing, no 🚨

Cash book **+3.13%** ($10,313) · signal-only (no cash/fees) was +4.96%. Starts YES **1/21**. Fills 32 · skips 32 · realized $+313.47.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=missing` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,313.47.

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
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | $61.71 | +41.20 | +29.60 | -3.00 | +38.20 |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | $44.06 | -0.81 | -18.90 | -51.03 | -51.84 |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | $53.03 | -54.24 | -38.16 | +112.00 | +57.76 |
| 2026-08-14 | `TGTX` | 25 | $47.94 | $47.27 | -16.75 | $48.74 | +36.75 | +20.00 | -60.75 | -24.00 |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | $12.78 | +40.28 | +44.52 | +74.20 | +114.48 |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | $28.15 | -42.00 | -26.04 | -24.78 | -66.78 |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | $1.09 | +246.88 | +293.17 | +185.16 | +432.04 |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | $22.72 | -10.60 | -21.73 | -21.73 | -32.33 |
| 2026-08-17 | `BTSG` | 20 | $61.71 | $61.69 | -0.40 | $60.38 | -26.20 | -26.60 | +37.80 | +11.60 |
| 2026-08-17 | `IREN` | 27 | $44.06 | $45.23 | +31.59 | $44.90 | -8.91 | +22.68 | -20.25 | -29.16 |
| 2026-08-17 | `TPG` | 24 | $53.03 | $52.67 | -8.64 | $51.77 | -21.60 | -30.24 | +49.12 | +27.52 |
| 2026-08-17 | `TGTX` | 25 | $48.74 | $48.74 | +0.00 | $49.28 | +13.50 | +13.50 | -24.00 | -10.50 |
| 2026-08-17 | `SLS` | 106 | $12.78 | $12.78 | +0.00 | $13.00 | +23.32 | +23.32 | +114.48 | +137.80 |
| 2026-08-17 | `HIMS` | 42 | $28.15 | $28.14 | -0.42 | $28.61 | +19.74 | +19.32 | -67.20 | -47.46 |
| 2026-08-17 | `INO` | 1543 | $1.09 | $1.07 | -30.86 | $1.15 | +123.44 | +92.58 | +401.18 | +524.62 |
| 2026-08-17 | `TNDM` | 53 | $22.72 | $22.50 | -11.66 | $22.25 | -12.99 | -24.65 | -43.99 | -56.97 |
| 2026-08-18 | `BTSG` | 20 | $60.38 | $60.00 | -7.60 | — | +0.00 | -7.60 | +4.00 | — |
| 2026-08-18 | `IREN` | 27 | $44.90 | $43.56 | -36.18 | — | +0.00 | -36.18 | -65.34 | — |
| 2026-08-18 | `TPG` | 24 | $51.77 | $51.77 | +0.00 | — | +0.00 | +0.00 | +27.52 | — |
| 2026-08-18 | `TGTX` | 25 | $49.28 | $49.28 | +0.00 | — | +0.00 | +0.00 | -10.50 | — |
| 2026-08-18 | `SLS` | 106 | $13.00 | $12.66 | -36.04 | — | +0.00 | -36.04 | +101.76 | — |
| 2026-08-18 | `HIMS` | 42 | $28.61 | $27.85 | -31.92 | — | +0.00 | -31.92 | -79.38 | — |
| 2026-08-18 | `INO` | 1543 | $1.15 | $1.14 | -15.43 | — | +0.00 | -15.43 | +509.19 | — |
| 2026-08-18 | `TNDM` | 53 | $22.25 | $22.16 | -5.03 | — | +0.00 | -5.03 | -62.01 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-21 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-24 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-27 | `CRK` | 89 | — | $14.42 | +0.00 | $14.62 | +17.80 | +17.80 | +0.00 | +17.80 |
| 2026-08-27 | `MOS` | 53 | — | $24.00 | +0.00 | $23.76 | -12.72 | -12.72 | +0.00 | -12.72 |
| 2026-08-27 | `SLI` | 497 | — | $2.60 | +0.00 | $2.64 | +19.88 | +19.88 | +0.00 | +19.88 |
| 2026-08-27 | `GGB` | 283 | — | $4.57 | +0.00 | $4.70 | +36.79 | +36.79 | +0.00 | +36.79 |
| 2026-08-27 | `MT` | 17 | — | $74.54 | +0.00 | $74.63 | +1.53 | +1.53 | +0.00 | +1.53 |
| 2026-08-27 | `TX` | 23 | — | $55.25 | +0.00 | $55.83 | +13.34 | +13.34 | +0.00 | +13.34 |
| 2026-08-27 | `ANET` | 6 | — | $205.90 | +0.00 | $201.09 | -28.86 | -28.86 | +0.00 | -28.86 |
| 2026-08-27 | `DLO` | 84 | — | $15.33 | +0.00 | $15.14 | -15.96 | -15.96 | +0.00 | -15.96 |
| 2026-08-28 | `CRK` | 89 | $14.62 | $14.63 | +0.89 | $14.29 | -30.26 | -29.37 | +18.69 | -11.57 |
| 2026-08-28 | `MOS` | 53 | $23.76 | $23.95 | +10.07 | $23.60 | -18.55 | -8.48 | -2.65 | -21.20 |
| 2026-08-28 | `SLI` | 497 | $2.64 | $2.68 | +19.88 | $2.55 | -64.61 | -44.73 | +39.76 | -24.85 |
| 2026-08-28 | `GGB` | 283 | $4.70 | $4.67 | -8.49 | $4.59 | -22.64 | -31.13 | +28.30 | +5.66 |
| 2026-08-28 | `MT` | 17 | $74.63 | $75.39 | +12.92 | $74.39 | -17.00 | -4.08 | +14.45 | -2.55 |
| 2026-08-28 | `TX` | 23 | $55.83 | $55.97 | +3.22 | $54.84 | -25.99 | -22.77 | +16.56 | -9.43 |
| 2026-08-28 | `ANET` | 6 | $201.09 | $200.00 | -6.54 | $195.38 | -27.72 | -34.26 | -35.40 | -63.12 |
| 2026-08-28 | `DLO` | 84 | $15.14 | $15.19 | +4.20 | $15.03 | -13.44 | -9.24 | -11.76 | -25.20 |
| 2026-08-31 | `CRK` | 89 | $14.29 | $14.54 | +22.25 | $14.43 | -9.79 | +12.46 | +10.68 | +0.89 |
| 2026-08-31 | `MOS` | 53 | $23.60 | $23.68 | +4.24 | $24.12 | +23.32 | +27.56 | -16.96 | +6.36 |
| 2026-08-31 | `SLI` | 497 | $2.55 | $2.58 | +14.91 | $2.67 | +44.73 | +59.64 | -9.94 | +34.79 |
| 2026-08-31 | `GGB` | 283 | $4.59 | $4.67 | +22.64 | $4.61 | -16.98 | +5.66 | +28.30 | +11.32 |
| 2026-08-31 | `MT` | 17 | $74.39 | $75.18 | +13.43 | $74.60 | -9.86 | +3.57 | +10.88 | +1.02 |
| 2026-08-31 | `TX` | 23 | $54.84 | $55.26 | +9.66 | $54.82 | -10.12 | -0.46 | +0.23 | -9.89 |
| 2026-08-31 | `ANET` | 6 | $195.38 | $195.50 | +0.72 | $195.69 | +1.14 | +1.86 | -62.40 | -61.26 |
| 2026-08-31 | `DLO` | 84 | $15.03 | $14.99 | -3.36 | $14.83 | -13.44 | -16.80 | -28.56 | -42.00 |
| 2026-09-01 | `CRK` | 89 | $14.43 | $15.82 | +123.71 | — | +0.00 | +123.71 | +124.60 | — |
| 2026-09-01 | `MOS` | 53 | $24.12 | $23.94 | -9.54 | — | +0.00 | -9.54 | -3.18 | — |
| 2026-09-01 | `SLI` | 497 | $2.67 | $2.67 | +0.00 | — | +0.00 | +0.00 | +34.79 | — |
| 2026-09-01 | `GGB` | 283 | $4.61 | $4.57 | -11.32 | — | +0.00 | -11.32 | +0.00 | — |
| 2026-09-01 | `MT` | 17 | $74.60 | $73.22 | -23.46 | — | +0.00 | -23.46 | -22.44 | — |
| 2026-09-01 | `TX` | 23 | $54.82 | $54.76 | -1.38 | — | +0.00 | -1.38 | -11.27 | — |
| 2026-09-01 | `ANET` | 6 | $195.69 | $195.77 | +0.48 | — | +0.00 | +0.48 | -60.78 | — |
| 2026-09-01 | `DLO` | 84 | $14.83 | $14.61 | -18.48 | — | +0.00 | -18.48 | -60.48 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-08 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +185.07 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | +257.46 | — | — | $97.53 | $10,435.58 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-17 | +2.25 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,415.19 | -20.39 | +110.30 | — | — | $97.53 | $10,525.50 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-18 | -6.20 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,393.29 | -132.21 | +0.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $10,358.15 | $10,358.15 | — |
| 2026-08-19 | -7.20 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-08-20 | +1.12 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-08-21 | +3.25 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-08-24 | -5.17 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-08-25 | +1.80 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-08-26 | +2.02 | $10,358.15 | — | $10,358.15 | +0.00 | +0.00 | — | — | $10,358.15 | $10,358.15 | — |
| 2026-08-27 | — | $10,358.15 | — | $10,358.15 | +0.00 | +31.80 | CRK, MOS, SLI, GGB, MT, TX, ANET, DLO | — | $133.39 | $10,367.13 | CRK×89, MOS×53, SLI×497, GGB×283, MT×17, TX×23, ANET×6, DLO×84 |
| 2026-08-28 | +0.75 | $133.39 | CRK×89, MOS×53, SLI×497, GGB×283, MT×17, TX×23, ANET×6, DLO×84 | $10,403.28 | +36.15 | -220.21 | — | — | $133.39 | $10,183.07 | CRK×89, MOS×53, SLI×497, GGB×283, MT×17, TX×23, ANET×6, DLO×84 |
| 2026-08-31 | -5.85 | $133.39 | CRK×89, MOS×53, SLI×497, GGB×283, MT×17, TX×23, ANET×6, DLO×84 | $10,267.56 | +84.49 | +9.00 | — | — | $133.39 | $10,276.56 | CRK×89, MOS×53, SLI×497, GGB×283, MT×17, TX×23, ANET×6, DLO×84 |
| 2026-09-01 | -6.30 | $133.39 | CRK×89, MOS×53, SLI×497, GGB×283, MT×17, TX×23, ANET×6, DLO×84 | $10,336.57 | +60.01 | +0.00 | — | CRK, MOS, SLI, GGB, MT, TX, ANET, DLO | $10,313.47 | $10,313.47 | — |
| 2026-09-02 | -3.83 | $10,313.47 | — | $10,313.47 | +0.00 | +0.00 | — | — | $10,313.47 | $10,313.47 | — |
| 2026-09-03 | -0.90 | $10,313.47 | — | $10,313.47 | +0.00 | +0.00 | — | — | $10,313.47 | $10,313.47 | — |
| 2026-09-04 | +2.25 | $10,313.47 | — | $10,313.47 | +0.00 | +0.00 | — | — | $10,313.47 | $10,313.47 | — |
| 2026-09-08 | -11.47 | $10,313.47 | — | $10,313.47 | +0.00 | +0.00 | — | — | $10,313.47 | $10,313.47 | — |
| 2026-09-09 | -13.95 | $10,313.47 | — | $10,313.47 | +0.00 | +0.00 | — | — | $10,313.47 | $10,313.47 | — |
| 2026-09-10 | -13.28 | $10,313.47 | — | $10,313.47 | +0.00 | +0.00 | — | — | $10,313.47 | $10,313.47 | — |
| 2026-09-11 | +0.50 | $10,313.47 | — | $10,313.47 | +0.00 | +0.00 | — | — | $10,313.47 | $10,313.47 | — |

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
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,435.58 vs 09:30 $10,178.12 (session +257.46) | 16:00 close · cash $97.53 · equity $10,435.58 vs 09:30 $10,178.12 (+257.46; session marks +257.46) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $59.65 → close $61.71 +41.20; IREN×27 09:30 $44.09 → close $44.06 -0.81; TPG×24 09:30 $55.29 → close $53.03 -54.24; TGTX×25 09:30 $47.27 → close $48.74 +36.75; SLS×106 09:30 $12.40 → close $12.78 +40.28; HIMS×42 09:30 $29.15 → close $28.15 -42.00; INO×1543 09:30 $0.93 → close $1.09 +246.88; TNDM×53 09:30 $22.92 → close $22.72 -10.60 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▼ 09:30 equity $10,415.19 vs yday $10,435.58 (-20.39) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,415.19 vs prior close $10,435.58 (-20.39) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,525.50 vs 09:30 $10,415.19 (session +110.30) | 16:00 close · cash $97.53 · equity $10,525.50 vs 09:30 $10,415.19 (+110.31; session marks +110.30) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $61.69 → close $60.38 -26.20; IREN×27 09:30 $45.23 → close $44.90 -8.91; TPG×24 09:30 $52.67 → close $51.77 -21.60; TGTX×25 09:30 $48.74 → close $49.28 +13.50; SLS×106 09:30 $12.78 → close $13.00 +23.32; HIMS×42 09:30 $28.14 → close $28.61 +19.74; INO×1543 09:30 $1.07 → close $1.15 +123.44; TNDM×53 09:30 $22.50 → close $22.25 -12.99 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▼ 09:30 equity $10,393.29 vs yday $10,525.50 (-132.21) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,393.29 vs prior close $10,525.50 (-132.21) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,295.46 | ▼ -0.12 after sell → book $10,391.22; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,469.49 | ▼ -69.50 after sell → book $10,389.13; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,709.89 | ▲ +23.38 after sell → book $10,387.05; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,939.81 | ▼ -14.65 after sell → book $10,384.97; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,279.43 | ▲ +97.12 after sell → book $10,382.63; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,446.99 | ▼ -83.63 after sell → book $10,380.49; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $9,185.84 | ▲ +471.89 after sell → book $10,360.32; vs 09:30 mark -20.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $10,358.15 | ▼ -66.33 after sell → book $10,358.15; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,393.29 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,358.15 | ▲ close $10,358.15 vs 09:30 $10,358.15 (session +0.00) | 16:00 close · cash $10,358.15 · no lots left · equity $10,358.15. | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,358.15 | ▲ 09:30 equity $10,358.15 vs yday $10,358.15 (+0.00) | 09:30 open · cash $10,358.15 · no holdings · equity $10,358.15 vs prior close $10,358.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 89 | $14.42 | $2.26 | — | $9,072.51 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ret5=+7.1; leftover $1294.77 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 53 | $24.00 | $2.15 | — | $7,798.36 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ret5=+8.7; leftover $1294.77 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 497 | $2.60 | $6.41 | — | $6,499.75 | — | union ∩ news_missing, no 🚨; gate news=missing; list flatten; ret5=+13.0; leftover $1294.77 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 283 | $4.57 | $3.65 | — | $5,202.79 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=+1.1; leftover $1294.77 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $74.54 | $2.04 | — | $3,933.57 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=-0.1; leftover $1294.77 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.25 | $2.06 | — | $2,660.76 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=+2.1; leftover $1294.77 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $205.90 | $2.01 | — | $1,423.35 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=+8.5; leftover $1294.77 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 84 | $15.33 | $2.24 | — | $133.39 | — | union ∩ news_missing, no 🚨; gate news=missing; list mover_buy; 🔵; ret5=+7.4; leftover $1294.77 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.39 | ▲ close $10,367.13 vs 09:30 $10,358.15 (session +31.80) | 16:00 close · cash $133.39 · equity $10,367.13 vs 09:30 $10,358.15 (+8.98; session marks +31.80) · 8 name(s) marked open→close (per-name table). CRK×89 09:30 $14.42 → close $14.62 +17.80; MOS×53 09:30 $24.00 → close $23.76 -12.72; SLI×497 09:30 $2.60 → close $2.64 +19.88; GGB×283 09:30 $4.57 → close $4.70 +36.79; MT×17 09:30 $74.54 → close $74.63 +1.53; TX×23 09:30 $55.25 → close $55.83 +13.34; ANET×6 09:30 $205.90 → close $201.09 -28.86; DLO×84 09:30 $15.33 → close $15.14 -15.96 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.39 | ▲ 09:30 equity $10,403.28 vs yday $10,367.13 (+36.15) | 09:30 open · cash $133.39 (unchanged overnight, no fees) · equity $10,403.28 vs prior close $10,367.13 (+36.15) · 8 name(s) re-marked at the open (per-name table). CRK×89 yday $14.62 → 09:30 $14.63 +0.89; MOS×53 yday $23.76 → 09:30 $23.95 +10.07; SLI×497 yday $2.64 → 09:30 $2.68 +19.88; GGB×283 yday $4.70 → 09:30 $4.67 -8.49; MT×17 yday $74.63 → 09:30 $75.39 +12.92; TX×23 yday $55.83 → 09:30 $55.97 +3.22; ANET×6 yday $201.09 → 09:30 $200.00 -6.54; DLO×84 yday $15.14 → 09:30 $15.19 +4.20 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.39 | ▼ close $10,183.07 vs 09:30 $10,403.28 (session -220.21) | 16:00 close · cash $133.39 · equity $10,183.07 vs 09:30 $10,403.28 (-220.21; session marks -220.21) · 8 name(s) marked open→close (per-name table). CRK×89 09:30 $14.63 → close $14.29 -30.26; MOS×53 09:30 $23.95 → close $23.60 -18.55; SLI×497 09:30 $2.68 → close $2.55 -64.61; GGB×283 09:30 $4.67 → close $4.59 -22.64; MT×17 09:30 $75.39 → close $74.39 -17.00; TX×23 09:30 $55.97 → close $54.84 -25.99; ANET×6 09:30 $200.00 → close $195.38 -27.72; DLO×84 09:30 $15.19 → close $15.03 -13.44 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.39 | ▲ 09:30 equity $10,267.56 vs yday $10,183.07 (+84.49) | 09:30 open · cash $133.39 (unchanged overnight, no fees) · equity $10,267.56 vs prior close $10,183.07 (+84.49) · 8 name(s) re-marked at the open (per-name table). CRK×89 yday $14.29 → 09:30 $14.54 +22.25; MOS×53 yday $23.60 → 09:30 $23.68 +4.24; SLI×497 yday $2.55 → 09:30 $2.58 +14.91; GGB×283 yday $4.59 → 09:30 $4.67 +22.64; MT×17 yday $74.39 → 09:30 $75.18 +13.43; TX×23 yday $54.84 → 09:30 $55.26 +9.66; ANET×6 yday $195.38 → 09:30 $195.50 +0.72; DLO×84 yday $15.03 → 09:30 $14.99 -3.36 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.39 | ▲ close $10,276.56 vs 09:30 $10,267.56 (session +9.00) | 16:00 close · cash $133.39 · equity $10,276.56 vs 09:30 $10,267.56 (+9.00; session marks +9.00) · 8 name(s) marked open→close (per-name table). CRK×89 09:30 $14.54 → close $14.43 -9.79; MOS×53 09:30 $23.68 → close $24.12 +23.32; SLI×497 09:30 $2.58 → close $2.67 +44.73; GGB×283 09:30 $4.67 → close $4.61 -16.98; MT×17 09:30 $75.18 → close $74.60 -9.86; TX×23 09:30 $55.26 → close $54.82 -10.12; ANET×6 09:30 $195.50 → close $195.69 +1.14; DLO×84 09:30 $14.99 → close $14.83 -13.44 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.39 | ▲ 09:30 equity $10,336.57 vs yday $10,276.56 (+60.01) | 09:30 open · cash $133.39 (unchanged overnight, no fees) · equity $10,336.57 vs prior close $10,276.56 (+60.01) · 8 name(s) re-marked at the open (per-name table). CRK×89 yday $14.43 → 09:30 $15.82 +123.71; MOS×53 yday $24.12 → 09:30 $23.94 -9.54; SLI×497 yday $2.67 → 09:30 $2.67 +0.00; GGB×283 yday $4.61 → 09:30 $4.57 -11.32; MT×17 yday $74.60 → 09:30 $73.22 -23.46; TX×23 yday $54.82 → 09:30 $54.76 -1.38; ANET×6 yday $195.69 → 09:30 $195.77 +0.48; DLO×84 yday $14.83 → 09:30 $14.61 -18.48 | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 89 | $15.82 | $2.28 | $+120.06 | $1,539.09 | ▲ +120.06 after sell → book $10,334.29; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 53 | $23.94 | $2.17 | $-7.50 | $2,805.74 | ▼ -7.50 after sell → book $10,332.12; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 497 | $2.67 | $6.50 | $+21.87 | $4,126.23 | ▲ +21.87 after sell → book $10,325.62; vs 09:30 mark -6.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 283 | $4.57 | $3.71 | $-7.36 | $5,415.83 | ▼ -7.36 after sell → book $10,321.91; vs 09:30 mark -3.71 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MT` | 17 | $73.22 | $2.06 | $-26.54 | $6,658.51 | ▼ -26.54 after sell → book $10,319.85; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TX` | 23 | $54.76 | $2.08 | $-15.41 | $7,915.91 | ▼ -15.41 after sell → book $10,317.77; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `ANET` | 6 | $195.77 | $2.03 | $-64.82 | $9,088.50 | ▼ -64.82 after sell → book $10,315.74; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 84 | $14.61 | $2.27 | $-64.99 | $10,313.47 | ▼ -64.99 after sell → book $10,313.47; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.47 | ▲ close $10,313.47 vs 09:30 $10,336.57 (session +0.00) | 16:00 close · cash $10,313.47 · no lots left · equity $10,313.47. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,313.47 | ▲ 09:30 equity $10,313.47 vs yday $10,313.47 (+0.00) | 09:30 open · cash $10,313.47 · no holdings · equity $10,313.47 vs prior close $10,313.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.47 | ▲ close $10,313.47 vs 09:30 $10,313.47 (session +0.00) | 16:00 close · cash $10,313.47 · no lots left · equity $10,313.47. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,313.47 | ▲ 09:30 equity $10,313.47 vs yday $10,313.47 (+0.00) | 09:30 open · cash $10,313.47 · no holdings · equity $10,313.47 vs prior close $10,313.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.47 | ▲ close $10,313.47 vs 09:30 $10,313.47 (session +0.00) | 16:00 close · cash $10,313.47 · no lots left · equity $10,313.47. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,313.47 | ▲ 09:30 equity $10,313.47 vs yday $10,313.47 (+0.00) | 09:30 open · cash $10,313.47 · no holdings · equity $10,313.47 vs prior close $10,313.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.47 | ▲ close $10,313.47 vs 09:30 $10,313.47 (session +0.00) | 16:00 close · cash $10,313.47 · no lots left · equity $10,313.47. | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,313.47 | ▲ 09:30 equity $10,313.47 vs yday $10,313.47 (+0.00) | 09:30 open · cash $10,313.47 · no holdings · equity $10,313.47 vs prior close $10,313.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.47 | ▲ close $10,313.47 vs 09:30 $10,313.47 (session +0.00) | 16:00 close · cash $10,313.47 · no lots left · equity $10,313.47. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,313.47 | ▲ 09:30 equity $10,313.47 vs yday $10,313.47 (+0.00) | 09:30 open · cash $10,313.47 · no holdings · equity $10,313.47 vs prior close $10,313.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.47 | ▲ close $10,313.47 vs 09:30 $10,313.47 (session +0.00) | 16:00 close · cash $10,313.47 · no lots left · equity $10,313.47. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,313.47 | ▲ 09:30 equity $10,313.47 vs yday $10,313.47 (+0.00) | 09:30 open · cash $10,313.47 · no holdings · equity $10,313.47 vs prior close $10,313.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.47 | ▲ close $10,313.47 vs 09:30 $10,313.47 (session +0.00) | 16:00 close · cash $10,313.47 · no lots left · equity $10,313.47. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,313.47 | ▲ 09:30 equity $10,313.47 vs yday $10,313.47 (+0.00) | 09:30 open · cash $10,313.47 · no holdings · equity $10,313.47 vs prior close $10,313.47 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.47 | ▲ close $10,313.47 vs 09:30 $10,313.47 (session +0.00) | 16:00 close · cash $10,313.47 · no lots left · equity $10,313.47. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `ANET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `DLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `DLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
