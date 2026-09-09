# Factor mine action — `union_vol_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ vol_g, no 🚨

Cash book **-0.69%** ($9,931) · signal-only (no cash/fees) was +17.72%. Starts YES **5/18**. Fills 134 · skips 43 · realized $-69.19.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the volume camera (is this name unusually active?) is green.
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
- **Gate** `vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,930.81.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `BTBT` | 833 | — | $1.50 | +0.00 | $1.57 | +58.31 | +58.31 | +0.00 | +58.31 |
| 2026-08-14 | `BETR` | 84 | — | $14.80 | +0.00 | $13.73 | -89.88 | -89.88 | +0.00 | -89.88 |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `HYLN` | 299 | — | $4.18 | +0.00 | $4.06 | -35.88 | -35.88 | +0.00 | -35.88 |
| 2026-08-14 | `ADUR` | 75 | — | $16.50 | +0.00 | $16.17 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `AIRO` | 112 | — | $11.12 | +0.00 | $9.57 | -173.60 | -173.60 | +0.00 | -173.60 |
| 2026-08-14 | `NCMI` | 464 | — | $2.69 | +0.00 | $2.86 | +78.88 | +78.88 | +0.00 | +78.88 |
| 2026-08-17 | `BTBT` | 833 | $1.57 | $1.52 | -41.65 | — | +0.00 | -41.65 | +16.66 | — |
| 2026-08-17 | `BETR` | 84 | $13.73 | $13.67 | -5.04 | — | +0.00 | -5.04 | -94.92 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `HYLN` | 299 | $4.06 | $4.10 | +11.96 | — | +0.00 | +11.96 | -23.92 | — |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | — | +0.00 | -33.00 | -57.75 | — |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `AIRO` | 112 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -173.60 | — |
| 2026-08-17 | `NCMI` | 464 | $2.86 | $2.80 | -27.84 | — | +0.00 | -27.84 | +51.04 | — |
| 2026-08-17 | `TMC` | 300 | — | $4.05 | +0.00 | $3.77 | -84.00 | -84.00 | +0.00 | -84.00 |
| 2026-08-17 | `CDNL` | 30 | — | $39.85 | +0.00 | $39.23 | -18.60 | -18.60 | +0.00 | -18.60 |
| 2026-08-17 | `ABX` | 133 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `VERA` | 38 | — | $31.30 | +0.00 | $31.63 | +12.54 | +12.54 | +0.00 | +12.54 |
| 2026-08-17 | `CAPR` | 177 | — | $6.87 | +0.00 | $7.45 | +102.66 | +102.66 | +0.00 | +102.66 |
| 2026-08-17 | `HTFL` | 29 | — | $41.23 | +0.00 | $41.94 | +20.59 | +20.59 | +0.00 | +20.59 |
| 2026-08-17 | `UMAC` | 37 | — | $32.55 | +0.00 | $30.15 | -88.80 | -88.80 | +0.00 | -88.80 |
| 2026-08-17 | `NPWR` | 633 | — | $1.92 | +0.00 | $1.73 | -120.27 | -120.27 | +0.00 | -120.27 |
| 2026-08-18 | `TMC` | 300 | $3.77 | $3.72 | -15.00 | — | +0.00 | -15.00 | -99.00 | — |
| 2026-08-18 | `CDNL` | 30 | $39.23 | $41.57 | +70.20 | — | +0.00 | +70.20 | +51.60 | — |
| 2026-08-18 | `ABX` | 133 | $9.12 | $9.03 | -11.97 | — | +0.00 | -11.97 | -11.97 | — |
| 2026-08-18 | `VERA` | 38 | $31.63 | $31.31 | -12.16 | — | +0.00 | -12.16 | +0.38 | — |
| 2026-08-18 | `CAPR` | 177 | $7.45 | $7.50 | +8.85 | — | +0.00 | +8.85 | +111.51 | — |
| 2026-08-18 | `HTFL` | 29 | $41.94 | $41.50 | -12.76 | — | +0.00 | -12.76 | +7.83 | — |
| 2026-08-18 | `UMAC` | 37 | $30.15 | $28.59 | -57.72 | — | +0.00 | -57.72 | -146.52 | — |
| 2026-08-18 | `NPWR` | 633 | $1.73 | $1.70 | -18.99 | — | +0.00 | -18.99 | -139.26 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 57 | — | $20.55 | +0.00 | $21.19 | +36.48 | +36.48 | +0.00 | +36.48 |
| 2026-08-20 | `BHP` | 12 | — | $91.01 | +0.00 | $93.63 | +31.44 | +31.44 | +0.00 | +31.44 |
| 2026-08-20 | `CDE` | 57 | — | $20.65 | +0.00 | $21.11 | +26.22 | +26.22 | +0.00 | +26.22 |
| 2026-08-20 | `HDSN` | 204 | — | $5.77 | +0.00 | $5.57 | -40.80 | -40.80 | +0.00 | -40.80 |
| 2026-08-20 | `IAG` | 60 | — | $19.63 | +0.00 | $20.50 | +52.20 | +52.20 | +0.00 | +52.20 |
| 2026-08-20 | `KGC` | 39 | — | $29.63 | +0.00 | $31.43 | +70.20 | +70.20 | +0.00 | +70.20 |
| 2026-08-20 | `NFGC` | 675 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 57 | $21.19 | $21.90 | +40.47 | — | +0.00 | +40.47 | +76.95 | — |
| 2026-08-21 | `BHP` | 12 | $93.63 | $95.72 | +25.08 | — | +0.00 | +25.08 | +56.52 | — |
| 2026-08-21 | `CDE` | 57 | $21.11 | $21.75 | +36.48 | — | +0.00 | +36.48 | +62.70 | — |
| 2026-08-21 | `HDSN` | 204 | $5.57 | $5.67 | +20.40 | — | +0.00 | +20.40 | -20.40 | — |
| 2026-08-21 | `IAG` | 60 | $20.50 | $21.17 | +40.20 | — | +0.00 | +40.20 | +92.40 | — |
| 2026-08-21 | `KGC` | 39 | $31.43 | $32.17 | +28.86 | — | +0.00 | +28.86 | +99.06 | — |
| 2026-08-21 | `NFGC` | 675 | $1.75 | $1.79 | +27.00 | — | +0.00 | +27.00 | +27.00 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 71 | — | $17.20 | +0.00 | $16.65 | -39.05 | -39.05 | +0.00 | -39.05 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 111 | — | $11.13 | +0.00 | $13.45 | +257.52 | +257.52 | +0.00 | +257.52 |
| 2026-08-21 | `AUTL` | 500 | — | $2.47 | +0.00 | $2.41 | -30.00 | -30.00 | +0.00 | -30.00 |
| 2026-08-21 | `CRDL` | 640 | — | $1.93 | +0.00 | $1.86 | -44.80 | -44.80 | +0.00 | -44.80 |
| 2026-08-21 | `CRSP` | 20 | — | $59.72 | +0.00 | $59.50 | -4.40 | -4.40 | +0.00 | -4.40 |
| 2026-08-21 | `CYPH` | 936 | — | $1.32 | +0.00 | $1.42 | +93.60 | +93.60 | +0.00 | +93.60 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.51 | -7.10 | — | +0.00 | -7.10 | +10.80 | — |
| 2026-08-24 | `AUPH` | 71 | $16.65 | $16.57 | -5.68 | — | +0.00 | -5.68 | -44.73 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 111 | $13.45 | $13.33 | -13.32 | — | +0.00 | -13.32 | +244.20 | — |
| 2026-08-24 | `AUTL` | 500 | $2.41 | $2.40 | -5.00 | — | +0.00 | -5.00 | -35.00 | — |
| 2026-08-24 | `CRDL` | 640 | $1.86 | $1.88 | +12.80 | — | +0.00 | +12.80 | -32.00 | — |
| 2026-08-24 | `CRSP` | 20 | $59.50 | $58.75 | -15.00 | — | +0.00 | -15.00 | -19.40 | — |
| 2026-08-24 | `CYPH` | 936 | $1.42 | $1.83 | +383.76 | — | +0.00 | +383.76 | +477.36 | — |
| 2026-08-25 | `CAPR` | 179 | — | $7.25 | +0.00 | $8.29 | +186.16 | +186.16 | +0.00 | +186.16 |
| 2026-08-25 | `KURA` | 95 | — | $13.59 | +0.00 | $13.59 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CCOI` | 137 | — | $9.49 | +0.00 | $9.88 | +53.43 | +53.43 | +0.00 | +53.43 |
| 2026-08-25 | `LIFE` | 35 | — | $36.96 | +0.00 | $38.56 | +56.00 | +56.00 | +0.00 | +56.00 |
| 2026-08-25 | `ZIP` | 286 | — | $4.55 | +0.00 | $4.35 | -57.20 | -57.20 | +0.00 | -57.20 |
| 2026-08-25 | `BMEA` | 798 | — | $1.63 | +0.00 | $1.73 | +79.80 | +79.80 | +0.00 | +79.80 |
| 2026-08-25 | `NPWR` | 650 | — | $2.00 | +0.00 | $1.95 | -32.50 | -32.50 | +0.00 | -32.50 |
| 2026-08-25 | `PUSA` | 340 | — | $3.80 | +0.00 | $3.78 | -6.80 | -6.80 | +0.00 | -6.80 |
| 2026-08-26 | `CAPR` | 179 | $8.29 | $8.29 | +0.00 | — | +0.00 | +0.00 | +186.16 | — |
| 2026-08-26 | `KURA` | 95 | $13.59 | $13.63 | +3.80 | — | +0.00 | +3.80 | +3.80 | — |
| 2026-08-26 | `CCOI` | 137 | $9.88 | $9.89 | +1.37 | — | +0.00 | +1.37 | +54.80 | — |
| 2026-08-26 | `LIFE` | 35 | $38.56 | $38.24 | -11.20 | — | +0.00 | -11.20 | +44.80 | — |
| 2026-08-26 | `ZIP` | 286 | $4.35 | $4.31 | -11.44 | — | +0.00 | -11.44 | -68.64 | — |
| 2026-08-26 | `BMEA` | 798 | $1.73 | $1.75 | +19.95 | — | +0.00 | +19.95 | +99.75 | — |
| 2026-08-26 | `NPWR` | 650 | $1.95 | $1.93 | -13.00 | — | +0.00 | -13.00 | -45.50 | — |
| 2026-08-26 | `PUSA` | 340 | $3.78 | $3.83 | +18.70 | — | +0.00 | +18.70 | +11.90 | — |
| 2026-08-26 | `SLQT` | 6077 | — | $0.58 | +0.00 | $0.55 | -200.54 | -200.54 | +0.00 | -200.54 |
| 2026-08-26 | `USDE` | 609 | — | $5.81 | +0.00 | $5.98 | +103.53 | +103.53 | +0.00 | +103.53 |
| 2026-08-26 | `DKS` | 28 | — | $121.87 | +0.00 | $129.66 | +218.12 | +218.12 | +0.00 | +218.12 |
| 2026-08-27 | `SLQT` | 6077 | $0.55 | $0.53 | -121.54 | — | +0.00 | -121.54 | -322.08 | — |
| 2026-08-27 | `USDE` | 609 | $5.98 | $6.50 | +316.68 | — | +0.00 | +316.68 | +420.21 | — |
| 2026-08-27 | `DKS` | 28 | $129.66 | $128.73 | -26.04 | — | +0.00 | -26.04 | +192.08 | — |
| 2026-08-28 | `SEDG` | 41 | — | $32.90 | +0.00 | $31.41 | -61.09 | -61.09 | +0.00 | -61.09 |
| 2026-08-28 | `URBN` | 16 | — | $79.42 | +0.00 | $81.09 | +26.72 | +26.72 | +0.00 | +26.72 |
| 2026-08-28 | `ANF` | 9 | — | $146.07 | +0.00 | $148.42 | +21.15 | +21.15 | +0.00 | +21.15 |
| 2026-08-28 | `BHVN` | 84 | — | $15.88 | +0.00 | $15.41 | -39.48 | -39.48 | +0.00 | -39.48 |
| 2026-08-28 | `BZ` | 74 | — | $18.15 | +0.00 | $17.80 | -25.90 | -25.90 | +0.00 | -25.90 |
| 2026-08-28 | `CAPR` | 138 | — | $9.73 | +0.00 | $9.59 | -19.32 | -19.32 | +0.00 | -19.32 |
| 2026-08-28 | `SMTC` | 9 | — | $141.76 | +0.00 | $131.17 | -95.31 | -95.31 | +0.00 | -95.31 |
| 2026-08-28 | `ERAS` | 70 | — | $19.25 | +0.00 | $18.03 | -85.40 | -85.40 | +0.00 | -85.40 |
| 2026-08-31 | `SEDG` | 41 | $31.41 | $31.15 | -10.66 | — | +0.00 | -10.66 | -71.75 | — |
| 2026-08-31 | `URBN` | 16 | $81.09 | $80.44 | -10.40 | — | +0.00 | -10.40 | +16.32 | — |
| 2026-08-31 | `ANF` | 9 | $148.42 | $148.03 | -3.51 | — | +0.00 | -3.51 | +17.64 | — |
| 2026-08-31 | `BHVN` | 84 | $15.41 | $15.46 | +4.20 | — | +0.00 | +4.20 | -35.28 | — |
| 2026-08-31 | `BZ` | 74 | $17.80 | $17.70 | -7.40 | — | +0.00 | -7.40 | -33.30 | — |
| 2026-08-31 | `CAPR` | 138 | $9.59 | $9.50 | -12.42 | — | +0.00 | -12.42 | -31.74 | — |
| 2026-08-31 | `SMTC` | 9 | $131.17 | $132.30 | +10.17 | — | +0.00 | +10.17 | -85.14 | — |
| 2026-08-31 | `ERAS` | 70 | $18.03 | $17.87 | -11.20 | — | +0.00 | -11.20 | -96.60 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `RVTY` | 9 | — | $132.45 | +0.00 | $130.63 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-03 | `CRK` | 84 | — | $15.45 | +0.00 | $14.95 | -42.00 | -42.00 | +0.00 | -42.00 |
| 2026-09-03 | `MRNA` | 8 | — | $145.94 | +0.00 | $148.87 | +23.40 | +23.40 | +0.00 | +23.40 |
| 2026-09-03 | `ARCT` | 77 | — | $16.77 | +0.00 | $15.56 | -93.17 | -93.17 | +0.00 | -93.17 |
| 2026-09-03 | `EIX` | 23 | — | $55.42 | +0.00 | $56.30 | +20.24 | +20.24 | +0.00 | +20.24 |
| 2026-09-03 | `CRDL` | 598 | — | $2.18 | +0.00 | $2.16 | -11.96 | -11.96 | +0.00 | -11.96 |
| 2026-09-03 | `GPRO` | 733 | — | $1.78 | +0.00 | $1.39 | -285.87 | -285.87 | +0.00 | -285.87 |
| 2026-09-03 | `FRVO` | 71 | — | $18.28 | +0.00 | $17.16 | -79.52 | -79.52 | +0.00 | -79.52 |
| 2026-09-04 | `RVTY` | 9 | $130.63 | $130.03 | -5.40 | — | +0.00 | -5.40 | -21.78 | — |
| 2026-09-04 | `CRK` | 84 | $14.95 | $15.00 | +4.20 | — | +0.00 | +4.20 | -37.80 | — |
| 2026-09-04 | `MRNA` | 8 | $148.87 | $153.62 | +38.00 | — | +0.00 | +38.00 | +61.40 | — |
| 2026-09-04 | `ARCT` | 77 | $15.56 | $15.61 | +3.85 | — | +0.00 | +3.85 | -89.32 | — |
| 2026-09-04 | `EIX` | 23 | $56.30 | $55.79 | -11.73 | — | +0.00 | -11.73 | +8.51 | — |
| 2026-09-04 | `CRDL` | 598 | $2.16 | $2.16 | +0.00 | — | +0.00 | +0.00 | -11.96 | — |
| 2026-09-04 | `GPRO` | 733 | $1.39 | $1.48 | +65.97 | — | +0.00 | +65.97 | -219.90 | — |
| 2026-09-04 | `FRVO` | 71 | $17.16 | $17.27 | +7.81 | — | +0.00 | +7.81 | -71.71 | — |
| 2026-09-04 | `CABA` | 361 | — | $3.46 | +0.00 | $3.47 | +3.61 | +3.61 | +0.00 | +3.61 |
| 2026-09-04 | `ALEC` | 495 | — | $2.52 | +0.00 | $2.46 | -29.70 | -29.70 | +0.00 | -29.70 |
| 2026-09-04 | `BHC` | 186 | — | $6.71 | +0.00 | $6.56 | -27.90 | -27.90 | +0.00 | -27.90 |
| 2026-09-04 | `BMEA` | 657 | — | $1.90 | +0.00 | $2.03 | +85.41 | +85.41 | +0.00 | +85.41 |
| 2026-09-04 | `OABI` | 261 | — | $4.78 | +0.00 | $4.33 | -117.45 | -117.45 | +0.00 | -117.45 |
| 2026-09-04 | `OPK` | 785 | — | $1.59 | +0.00 | $1.64 | +39.25 | +39.25 | +0.00 | +39.25 |
| 2026-09-04 | `VIR` | 110 | — | $11.31 | +0.00 | $11.38 | +8.25 | +8.25 | +0.00 | +8.25 |
| 2026-09-04 | `EOSE` | 347 | — | $3.52 | +0.00 | $3.88 | +124.92 | +124.92 | +0.00 | +124.92 |
| 2026-09-08 | `CABA` | 361 | $3.47 | $3.43 | -14.44 | — | +0.00 | -14.44 | -10.83 | — |
| 2026-09-08 | `ALEC` | 495 | $2.46 | $2.38 | -39.60 | — | +0.00 | -39.60 | -69.30 | — |
| 2026-09-08 | `BHC` | 186 | $6.56 | $6.57 | +1.86 | — | +0.00 | +1.86 | -26.04 | — |
| 2026-09-08 | `BMEA` | 657 | $2.03 | $2.00 | -19.71 | — | +0.00 | -19.71 | +65.70 | — |
| 2026-09-08 | `OABI` | 261 | $4.33 | $4.30 | -7.83 | — | +0.00 | -7.83 | -125.28 | — |
| 2026-09-08 | `OPK` | 785 | $1.64 | $1.63 | -7.85 | — | +0.00 | -7.85 | +31.40 | — |
| 2026-09-08 | `VIR` | 110 | $11.38 | $11.22 | -18.15 | — | +0.00 | -18.15 | -9.90 | — |
| 2026-09-08 | `EOSE` | 347 | $3.88 | $3.99 | +38.17 | — | +0.00 | +38.17 | +163.09 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -168.89 | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | — | $10.28 | $9,797.82 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 |
| 2026-08-17 | +2.25 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,768.32 | -29.50 | -175.88 | TMC, CDNL, ABX, VERA, CAPR, HTFL, UMAC, NPWR | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | $65.04 | $9,533.39 | TMC×300, CDNL×30, ABX×133, VERA×38, CAPR×177, HTFL×29, UMAC×37, NPWR×633 |
| 2026-08-18 | -6.20 | $65.04 | TMC×300, CDNL×30, ABX×133, VERA×38, CAPR×177, HTFL×29, UMAC×37, NPWR×633 | $9,483.84 | -49.55 | +0.00 | — | TMC, CDNL, ABX, VERA, CAPR, HTFL, UMAC, NPWR | $9,458.21 | $9,458.21 | — |
| 2026-08-19 | -7.20 | $9,458.21 | — | $9,458.21 | -0.00 | +0.00 | — | — | $9,458.21 | $9,458.21 | — |
| 2026-08-20 | +1.12 | $9,458.21 | — | $9,458.21 | -0.00 | +221.42 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $145.69 | $9,655.65 | AG×57, BHP×12, CDE×57, HDSN×204, IAG×60, KGC×39, NFGC×675, WPM×8 |
| 2026-08-21 | +3.25 | $145.69 | AG×57, BHP×12, CDE×57, HDSN×204, IAG×60, KGC×39, NFGC×675, WPM×8 | $9,909.74 | +254.09 | +249.57 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $215.54 | $10,097.66 | AU×10, AUPH×71, AEM×5, ARCT×111, AUTL×500, CRDL×640, CRSP×20, CYPH×936 |
| 2026-08-24 | -5.17 | $215.54 | AU×10, AUPH×71, AEM×5, ARCT×111, AUTL×500, CRDL×640, CRSP×20, CYPH×936 | $10,452.97 | +355.31 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,415.10 | $10,415.10 | — |
| 2026-08-25 | +1.80 | $10,415.10 | — | $10,415.10 | -0.00 | +278.89 | CAPR, KURA, CCOI, LIFE, ZIP, BMEA, NPWR, PUSA | — | $2.48 | $10,657.94 | CAPR×179, KURA×95, CCOI×137, LIFE×35, ZIP×286, BMEA×798, NPWR×650, PUSA×340 |
| 2026-08-26 | +2.02 | $2.48 | CAPR×179, KURA×95, CCOI×137, LIFE×35, ZIP×286, BMEA×798, NPWR×650, PUSA×340 | $10,666.12 | +8.18 | +121.11 | SLQT, USDE, DKS | CAPR, KURA, CCOI, LIFE, ZIP, BMEA, NPWR, PUSA | $72.43 | $10,687.08 | SLQT×6077, USDE×609, DKS×28 |
| 2026-08-27 | — | $72.43 | SLQT×6077, USDE×609, DKS×28 | $10,856.18 | +169.10 | +0.00 | — | SLQT, USDE, DKS | $10,794.60 | $10,794.60 | — |
| 2026-08-28 | +0.75 | $10,794.60 | — | $10,794.60 | +0.00 | -278.63 | SEDG, URBN, ANF, BHVN, BZ, CAPR, SMTC, ERAS | — | $200.01 | $10,498.73 | SEDG×41, URBN×16, ANF×9, BHVN×84, BZ×74, CAPR×138, SMTC×9, ERAS×70 |
| 2026-08-31 | -5.85 | $200.01 | SEDG×41, URBN×16, ANF×9, BHVN×84, BZ×74, CAPR×138, SMTC×9, ERAS×70 | $10,457.51 | -41.22 | +0.00 | — | SEDG, URBN, ANF, BHVN, BZ, CAPR, SMTC, ERAS | $10,440.08 | $10,440.08 | — |
| 2026-09-01 | -6.30 | $10,440.08 | — | $10,440.08 | +0.00 | +0.00 | — | — | $10,440.08 | $10,440.08 | — |
| 2026-09-02 | -3.83 | $10,440.08 | — | $10,440.08 | +0.00 | +0.00 | — | — | $10,440.08 | $10,440.08 | — |
| 2026-09-03 | -0.90 | $10,440.08 | — | $10,440.08 | +0.00 | -485.26 | RVTY, CRK, MRNA, ARCT, EIX, CRDL, GPRO, FRVO | — | $280.54 | $9,924.90 | RVTY×9, CRK×84, MRNA×8, ARCT×77, EIX×23, CRDL×598, GPRO×733, FRVO×71 |
| 2026-09-04 | +2.25 | $280.54 | RVTY×9, CRK×84, MRNA×8, ARCT×77, EIX×23, CRDL×598, GPRO×733, FRVO×71 | $10,027.60 | +102.70 | +86.39 | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, EOSE | RVTY, CRK, MRNA, ARCT, EIX, CRDL, GPRO, FRVO | $0.85 | $10,041.33 | CABA×361, ALEC×495, BHC×186, BMEA×657, OABI×261, OPK×785, VIR×110, EOSE×347 |
| 2026-09-08 | -11.47 | $0.85 | CABA×361, ALEC×495, BHC×186, BMEA×657, OABI×261, OPK×785, VIR×110, EOSE×347 | $9,973.78 | -67.55 | +0.00 | — | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, EOSE | $9,930.81 | $9,930.81 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▼ close $9,797.82 vs 09:30 $10,000.00 (session -168.89) | 16:00 close · cash $10.28 · equity $9,797.82 vs 09:30 $10,000.00 (-202.18; session marks -168.89) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.50 → close $1.57 +58.31; BETR×84 09:30 $14.80 → close $13.73 -89.88; ANGX×290 09:30 $4.31 → close $4.37 +17.40; HYLN×299 09:30 $4.18 → close $4.06 -35.88; ADUR×75 09:30 $16.50 → close $16.17 -24.75; ARX×63 09:30 $19.57 → close $19.58 +0.63; AIRO×112 09:30 $11.12 → close $9.57 -173.60; NCMI×464 09:30 $2.69 → close $2.86 +78.88 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,768.32 vs yday $9,797.82 (-29.50) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,768.32 vs prior close $9,797.82 (-29.50) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84 | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $1,265.54 | ▼ -4.98 after sell → book $9,757.42; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $2,411.56 | ▼ -99.43 after sell → book $9,755.16; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,741.76 | ▲ +76.56 after sell → book $9,751.36; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,963.74 | ▼ -31.69 after sell → book $9,747.44; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $6,141.25 | ▼ -62.20 after sell → book $9,745.20; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,371.97 | ▼ -4.38 after sell → book $9,743.01; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $8,441.45 | ▼ -178.28 after sell → book $9,740.65; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $9,734.58 | ▲ +38.98 after sell → book $9,734.58; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 300 | $4.05 | $3.87 | — | $8,515.71 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $7,318.13 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1216.82 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 133 | $9.12 | $2.39 | — | $6,102.78 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 38 | $31.30 | $2.10 | — | $4,911.27 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=-3.8; leftover $1216.82 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 177 | $6.87 | $2.52 | — | $3,692.76 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+62.6; leftover $1216.82 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $2,495.02 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+46.0; leftover $1216.82 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $1,288.57 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1216.82 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 633 | $1.92 | $8.17 | — | $65.04 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1216.82 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.04 | ▼ close $9,533.39 vs 09:30 $9,768.32 (session -175.88) | 16:00 close · cash $65.04 · equity $9,533.39 vs 09:30 $9,768.32 (-234.93; session marks -175.88) · 8 name(s) marked open→close (per-name table). TMC×300 09:30 $4.05 → close $3.77 -84.00; CDNL×30 09:30 $39.85 → close $39.23 -18.60; ABX×133 09:30 $9.12 → close $9.12 +0.00; VERA×38 09:30 $31.30 → close $31.63 +12.54; CAPR×177 09:30 $6.87 → close $7.45 +102.66; HTFL×29 09:30 $41.23 → close $41.94 +20.59; UMAC×37 09:30 $32.55 → close $30.15 -88.80; NPWR×633 09:30 $1.92 → close $1.73 -120.27 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.04 | ▼ 09:30 equity $9,483.84 vs yday $9,533.39 (-49.55) | 09:30 open · cash $65.04 (unchanged overnight, no fees) · equity $9,483.84 vs prior close $9,533.39 (-49.55) · 8 name(s) re-marked at the open (per-name table). TMC×300 yday $3.77 → 09:30 $3.72 -15.00; CDNL×30 yday $39.23 → 09:30 $41.57 +70.20; ABX×133 yday $9.12 → 09:30 $9.03 -11.97; VERA×38 yday $31.63 → 09:30 $31.31 -12.16; CAPR×177 yday $7.45 → 09:30 $7.50 +8.85; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72; NPWR×633 yday $1.73 → 09:30 $1.70 -18.99 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 300 | $3.72 | $3.93 | $-106.80 | $1,177.11 | ▼ -106.80 after sell → book $9,479.91; vs 09:30 mark -3.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $2,422.11 | ▲ +47.42 after sell → book $9,477.81; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 133 | $9.03 | $2.42 | $-16.78 | $3,620.68 | ▼ -16.78 after sell → book $9,475.39; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 38 | $31.31 | $2.12 | $-3.85 | $4,808.34 | ▼ -3.85 after sell → book $9,473.27; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 177 | $7.50 | $2.56 | $+106.43 | $6,133.27 | ▲ +106.43 after sell → book $9,470.70; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $7,334.68 | ▲ +3.66 after sell → book $9,468.61; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $8,390.39 | ▼ -150.74 after sell → book $9,466.49; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 633 | $1.70 | $8.28 | $-155.71 | $9,458.21 | ▼ -155.71 after sell → book $9,458.21; vs 09:30 mark -8.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.21 | ▲ close $9,458.21 vs 09:30 $9,483.84 (session +0.00) | 16:00 close · cash $9,458.21 · no lots left · equity $9,458.21. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.21 | ▲ 09:30 equity $9,458.21 vs yday $9,458.21 (-0.00) | 09:30 open · cash $9,458.21 · no holdings · equity $9,458.21 vs prior close $9,458.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.21 | ▲ close $9,458.21 vs 09:30 $9,458.21 (session +0.00) | 16:00 close · cash $9,458.21 · no lots left · equity $9,458.21. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.21 | ▲ 09:30 equity $9,458.21 vs yday $9,458.21 (-0.00) | 09:30 open · cash $9,458.21 · no holdings · equity $9,458.21 vs prior close $9,458.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,284.69 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $7,190.55 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $6,011.34 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 204 | $5.77 | $2.63 | — | $4,831.63 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,651.66 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $2,493.98 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 675 | $1.75 | $8.71 | — | $1,304.02 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $145.69 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.69 | ▲ close $9,655.65 vs 09:30 $9,458.21 (session +221.42) | 16:00 close · cash $145.69 · equity $9,655.65 vs 09:30 $9,458.21 (+197.44; session marks +221.42) · 8 name(s) marked open→close (per-name table). AG×57 09:30 $20.55 → close $21.19 +36.48; BHP×12 09:30 $91.01 → close $93.63 +31.44; CDE×57 09:30 $20.65 → close $21.11 +26.22; HDSN×204 09:30 $5.77 → close $5.57 -40.80; IAG×60 09:30 $19.63 → close $20.50 +52.20; KGC×39 09:30 $29.63 → close $31.43 +70.20; NFGC×675 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.69 | ▲ 09:30 equity $9,909.74 vs yday $9,655.65 (+254.09) | 09:30 open · cash $145.69 (unchanged overnight, no fees) · equity $9,909.74 vs prior close $9,655.65 (+254.09) · 8 name(s) re-marked at the open (per-name table). AG×57 yday $21.19 → 09:30 $21.90 +40.47; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; HDSN×204 yday $5.57 → 09:30 $5.67 +20.40; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×675 yday $1.75 → 09:30 $1.79 +27.00; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 57 | $21.90 | $2.18 | $+72.61 | $1,391.81 | ▲ +72.61 after sell → book $9,907.56; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 12 | $95.72 | $2.05 | $+52.45 | $2,538.40 | ▲ +52.45 after sell → book $9,905.51; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 57 | $21.75 | $2.18 | $+58.36 | $3,775.97 | ▲ +58.36 after sell → book $9,903.33; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 204 | $5.67 | $2.68 | $-25.71 | $4,929.97 | ▼ -25.71 after sell → book $9,900.65; vs 09:30 mark -2.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 60 | $21.17 | $2.19 | $+88.04 | $6,197.98 | ▲ +88.04 after sell → book $9,898.46; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $7,450.49 | ▲ +94.83 after sell → book $9,896.34; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 675 | $1.79 | $8.83 | $+9.46 | $8,649.91 | ▲ +9.46 after sell → book $9,887.51; vs 09:30 mark -8.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,885.47 | ▲ +77.23 after sell → book $9,885.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,689.15 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 71 | $17.20 | $2.20 | — | $7,465.75 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,382.24 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 111 | $11.13 | $2.32 | — | $5,144.49 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 500 | $2.47 | $6.45 | — | $3,903.04 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 640 | $1.93 | $8.26 | — | $2,659.59 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $1,463.14 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 936 | $1.32 | $12.07 | — | $215.54 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $215.54 | ▲ close $10,097.66 vs 09:30 $9,909.74 (session +249.57) | 16:00 close · cash $215.54 · equity $10,097.66 vs 09:30 $9,909.74 (+187.92; session marks +249.57) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×71 09:30 $17.20 → close $16.65 -39.05; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×111 09:30 $11.13 → close $13.45 +257.52; AUTL×500 09:30 $2.47 → close $2.41 -30.00; CRDL×640 09:30 $1.93 → close $1.86 -44.80; CRSP×20 09:30 $59.72 → close $59.50 -4.40; CYPH×936 09:30 $1.32 → close $1.42 +93.60 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $215.54 | ▲ 09:30 equity $10,452.97 vs yday $10,097.66 (+355.31) | 09:30 open · cash $215.54 (unchanged overnight, no fees) · equity $10,452.97 vs prior close $10,097.66 (+355.31) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.51 -7.10; AUPH×71 yday $16.65 → 09:30 $16.57 -5.68; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×111 yday $13.45 → 09:30 $13.33 -13.32; AUTL×500 yday $2.41 → 09:30 $2.40 -5.00; CRDL×640 yday $1.86 → 09:30 $1.88 +12.80; CRSP×20 yday $59.50 → 09:30 $58.75 -15.00; CYPH×936 yday $1.42 → 09:30 $1.83 +383.76 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,418.60 | ▲ +6.74 after sell → book $10,450.93; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 71 | $16.57 | $2.22 | $-49.16 | $2,592.85 | ▼ -49.16 after sell → book $10,448.71; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,675.97 | ▼ -0.38 after sell → book $10,446.68; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 111 | $13.33 | $2.35 | $+239.52 | $5,153.25 | ▲ +239.52 after sell → book $10,444.33; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 500 | $2.40 | $6.54 | $-47.99 | $6,346.71 | ▼ -47.99 after sell → book $10,437.79; vs 09:30 mark -6.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 640 | $1.88 | $8.37 | $-48.63 | $7,541.53 | ▼ -48.63 after sell → book $10,429.41; vs 09:30 mark -8.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 20 | $58.75 | $2.07 | $-23.52 | $8,714.46 | ▼ -23.52 after sell → book $10,427.34; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 936 | $1.83 | $12.24 | $+453.04 | $10,415.10 | ▲ +453.04 after sell → book $10,415.10; vs 09:30 mark -12.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,415.10 | ▲ close $10,415.10 vs 09:30 $10,452.97 (session +0.00) | 16:00 close · cash $10,415.10 · no lots left · equity $10,415.10. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,415.10 | ▲ 09:30 equity $10,415.10 vs yday $10,415.10 (-0.00) | 09:30 open · cash $10,415.10 · no holdings · equity $10,415.10 vs prior close $10,415.10 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 179 | $7.25 | $2.53 | — | $9,114.82 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1301.89 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 95 | $13.59 | $2.27 | — | $7,821.50 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1301.89 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 137 | $9.49 | $2.40 | — | $6,518.97 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1301.89 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 35 | $36.96 | $2.10 | — | $5,223.27 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1301.89 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 286 | $4.55 | $3.69 | — | $3,918.28 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1301.89 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 798 | $1.63 | $10.29 | — | $2,607.25 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1301.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 650 | $2.00 | $8.38 | — | $1,298.86 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $1301.89 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 340 | $3.80 | $4.39 | — | $2.48 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1301.89 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.48 | ▲ close $10,657.94 vs 09:30 $10,415.10 (session +278.89) | 16:00 close · cash $2.48 · equity $10,657.94 vs 09:30 $10,415.10 (+242.84; session marks +278.89) · 8 name(s) marked open→close (per-name table). CAPR×179 09:30 $7.25 → close $8.29 +186.16; KURA×95 09:30 $13.59 → close $13.59 +0.00; CCOI×137 09:30 $9.49 → close $9.88 +53.43; LIFE×35 09:30 $36.96 → close $38.56 +56.00; ZIP×286 09:30 $4.55 → close $4.35 -57.20; BMEA×798 09:30 $1.63 → close $1.73 +79.80; NPWR×650 09:30 $2.00 → close $1.95 -32.50; PUSA×340 09:30 $3.80 → close $3.78 -6.80 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.48 | ▲ 09:30 equity $10,666.12 vs yday $10,657.94 (+8.18) | 09:30 open · cash $2.48 (unchanged overnight, no fees) · equity $10,666.12 vs prior close $10,657.94 (+8.18) · 8 name(s) re-marked at the open (per-name table). CAPR×179 yday $8.29 → 09:30 $8.29 +0.00; KURA×95 yday $13.59 → 09:30 $13.63 +3.80; CCOI×137 yday $9.88 → 09:30 $9.89 +1.37; LIFE×35 yday $38.56 → 09:30 $38.24 -11.20; ZIP×286 yday $4.35 → 09:30 $4.31 -11.44; BMEA×798 yday $1.73 → 09:30 $1.75 +19.95; NPWR×650 yday $1.95 → 09:30 $1.93 -13.00; PUSA×340 yday $3.78 → 09:30 $3.83 +18.70 | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 179 | $8.29 | $2.57 | $+181.06 | $1,483.82 | ▲ +181.06 after sell → book $10,663.55; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟢 digest🟢 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 95 | $13.63 | $2.30 | $-0.78 | $2,776.37 | ▼ -0.78 after sell → book $10,661.25; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SELL** | `CCOI` | 137 | $9.89 | $2.43 | $+49.96 | $4,128.86 | ▲ +49.96 after sell → book $10,658.81; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 35 | $38.24 | $2.12 | $+40.59 | $5,465.15 | ▲ +40.59 after sell → book $10,656.70; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 286 | $4.31 | $3.75 | $-76.08 | $6,694.06 | ▼ -76.08 after sell → book $10,652.95; vs 09:30 mark -3.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 798 | $1.75 | $10.44 | $+79.02 | $8,084.11 | ▲ +79.02 after sell → book $10,642.51; vs 09:30 mark -10.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `NPWR` | 650 | $1.93 | $8.50 | $-62.39 | $9,330.11 | ▼ -62.39 after sell → book $10,634.01; vs 09:30 mark -8.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `PUSA` | 340 | $3.83 | $4.45 | $+3.06 | $10,629.56 | ▲ +3.06 after sell → book $10,629.56; vs 09:30 mark -4.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 6077 | $0.58 | $53.66 | — | $7,033.01 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_mover; 🔵; ret5=-27.5; leftover $3543.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 609 | $5.81 | $7.86 | — | $3,486.86 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $3543.19 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 28 | $121.87 | $2.07 | — | $72.43 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_mover; 🔵; ret5=-35.1; leftover $3543.19 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.43 | ▲ close $10,687.08 vs 09:30 $10,666.12 (session +121.11) | 16:00 close · cash $72.43 · equity $10,687.08 vs 09:30 $10,666.12 (+20.96; session marks +121.11) · 3 name(s) marked open→close (per-name table). SLQT×6077 09:30 $0.58 → close $0.55 -200.54; USDE×609 09:30 $5.81 → close $5.98 +103.53; DKS×28 09:30 $121.87 → close $129.66 +218.12 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.43 | ▲ 09:30 equity $10,856.18 vs yday $10,687.08 (+169.10) | 09:30 open · cash $72.43 (unchanged overnight, no fees) · equity $10,856.18 vs prior close $10,687.08 (+169.10) · 3 name(s) re-marked at the open (per-name table). SLQT×6077 yday $0.55 → 09:30 $0.53 -121.54; USDE×609 yday $5.98 → 09:30 $6.50 +316.68; DKS×28 yday $129.66 → 09:30 $128.73 -26.04 | — |
| 2026-08-27 09:30 ET | **SELL** | `SLQT` | 6077 | $0.53 | $51.47 | $-427.21 | $3,241.76 | ▼ -427.21 after sell → book $10,804.70; vs 09:30 mark -51.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 609 | $6.50 | $7.99 | $+404.37 | $7,192.27 | ▲ +404.37 after sell → book $10,796.71; vs 09:30 mark -7.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 28 | $128.73 | $2.11 | $+187.89 | $10,794.60 | ▲ +187.89 after sell → book $10,794.60; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,794.60 | ▲ close $10,794.60 vs 09:30 $10,856.18 (session +0.00) | 16:00 close · cash $10,794.60 · no lots left · equity $10,794.60. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,794.60 | ▲ 09:30 equity $10,794.60 vs yday $10,794.60 (+0.00) | 09:30 open · cash $10,794.60 · no holdings · equity $10,794.60 vs prior close $10,794.60 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 41 | $32.90 | $2.11 | — | $9,443.59 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1349.33 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $8,170.83 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1349.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $6,854.18 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1349.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 84 | $15.88 | $2.24 | — | $5,518.02 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+19.4; leftover $1349.33 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 74 | $18.15 | $2.21 | — | $4,172.71 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+14.1; leftover $1349.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 138 | $9.73 | $2.40 | — | $2,827.56 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+47.1; leftover $1349.33 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $1,549.71 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1349.33 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 70 | $19.25 | $2.20 | — | $200.01 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer; ret5=+14.1; leftover $1349.33 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $200.01 | ▼ close $10,498.73 vs 09:30 $10,794.60 (session -278.63) | 16:00 close · cash $200.01 · equity $10,498.73 vs 09:30 $10,794.60 (-295.87; session marks -278.63) · 8 name(s) marked open→close (per-name table). SEDG×41 09:30 $32.90 → close $31.41 -61.09; URBN×16 09:30 $79.42 → close $81.09 +26.72; ANF×9 09:30 $146.07 → close $148.42 +21.15; BHVN×84 09:30 $15.88 → close $15.41 -39.48; BZ×74 09:30 $18.15 → close $17.80 -25.90; CAPR×138 09:30 $9.73 → close $9.59 -19.32; SMTC×9 09:30 $141.76 → close $131.17 -95.31; ERAS×70 09:30 $19.25 → close $18.03 -85.40 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $200.01 | ▼ 09:30 equity $10,457.51 vs yday $10,498.73 (-41.22) | 09:30 open · cash $200.01 (unchanged overnight, no fees) · equity $10,457.51 vs prior close $10,498.73 (-41.22) · 8 name(s) re-marked at the open (per-name table). SEDG×41 yday $31.41 → 09:30 $31.15 -10.66; URBN×16 yday $81.09 → 09:30 $80.44 -10.40; ANF×9 yday $148.42 → 09:30 $148.03 -3.51; BHVN×84 yday $15.41 → 09:30 $15.46 +4.20; BZ×74 yday $17.80 → 09:30 $17.70 -7.40; CAPR×138 yday $9.59 → 09:30 $9.50 -12.42; SMTC×9 yday $131.17 → 09:30 $132.30 +10.17; ERAS×70 yday $18.03 → 09:30 $17.87 -11.20 | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 41 | $31.15 | $2.13 | $-76.00 | $1,475.02 | ▼ -76.00 after sell → book $10,455.37; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $2,760.01 | ▲ +12.22 after sell → book $10,453.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $4,090.24 | ▲ +13.59 after sell → book $10,451.28; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 84 | $15.46 | $2.27 | $-39.79 | $5,386.61 | ▼ -39.79 after sell → book $10,449.01; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 74 | $17.70 | $2.23 | $-37.75 | $6,694.18 | ▼ -37.75 after sell → book $10,446.78; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 138 | $9.50 | $2.44 | $-36.58 | $8,002.74 | ▼ -36.58 after sell → book $10,444.34; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $9,191.40 | ▼ -89.19 after sell → book $10,442.30; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 70 | $17.87 | $2.22 | $-101.02 | $10,440.08 | ▼ -101.02 after sell → book $10,440.08; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,440.08 | ▲ close $10,440.08 vs 09:30 $10,457.51 (session +0.00) | 16:00 close · cash $10,440.08 · no lots left · equity $10,440.08. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,440.08 | ▲ 09:30 equity $10,440.08 vs yday $10,440.08 (+0.00) | 09:30 open · cash $10,440.08 · no holdings · equity $10,440.08 vs prior close $10,440.08 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,440.08 | ▲ close $10,440.08 vs 09:30 $10,440.08 (session +0.00) | 16:00 close · cash $10,440.08 · no lots left · equity $10,440.08. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,440.08 | ▲ 09:30 equity $10,440.08 vs yday $10,440.08 (+0.00) | 09:30 open · cash $10,440.08 · no holdings · equity $10,440.08 vs prior close $10,440.08 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,440.08 | ▲ close $10,440.08 vs 09:30 $10,440.08 (session +0.00) | 16:00 close · cash $10,440.08 · no lots left · equity $10,440.08. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,440.08 | ▲ 09:30 equity $10,440.08 vs yday $10,440.08 (+0.00) | 09:30 open · cash $10,440.08 · no holdings · equity $10,440.08 vs prior close $10,440.08 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $9,246.01 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1305.01 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 84 | $15.45 | $2.24 | — | $7,945.97 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1305.01 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $6,776.40 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1305.01 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 77 | $16.77 | $2.22 | — | $5,482.89 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1305.01 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 23 | $55.42 | $2.06 | — | $4,206.17 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=-25.9; leftover $1305.01 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 598 | $2.18 | $7.71 | — | $2,894.81 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1305.01 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 733 | $1.78 | $9.46 | — | $1,580.62 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+183.1; leftover $1305.01 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 71 | $18.28 | $2.20 | — | $280.54 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+16.5; leftover $1305.01 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $280.54 | ▼ close $9,924.90 vs 09:30 $10,440.08 (session -485.26) | 16:00 close · cash $280.54 · equity $9,924.90 vs 09:30 $10,440.08 (-515.18; session marks -485.26) · 8 name(s) marked open→close (per-name table). RVTY×9 09:30 $132.45 → close $130.63 -16.38; CRK×84 09:30 $15.45 → close $14.95 -42.00; MRNA×8 09:30 $145.94 → close $148.87 +23.40; ARCT×77 09:30 $16.77 → close $15.56 -93.17; EIX×23 09:30 $55.42 → close $56.30 +20.24; CRDL×598 09:30 $2.18 → close $2.16 -11.96; GPRO×733 09:30 $1.78 → close $1.39 -285.87; FRVO×71 09:30 $18.28 → close $17.16 -79.52 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $280.54 | ▲ 09:30 equity $10,027.60 vs yday $9,924.90 (+102.70) | 09:30 open · cash $280.54 (unchanged overnight, no fees) · equity $10,027.60 vs prior close $9,924.90 (+102.70) · 8 name(s) re-marked at the open (per-name table). RVTY×9 yday $130.63 → 09:30 $130.03 -5.40; CRK×84 yday $14.95 → 09:30 $15.00 +4.20; MRNA×8 yday $148.87 → 09:30 $153.62 +38.00; ARCT×77 yday $15.56 → 09:30 $15.61 +3.85; EIX×23 yday $56.30 → 09:30 $55.79 -11.73; CRDL×598 yday $2.16 → 09:30 $2.16 +0.00; GPRO×733 yday $1.39 → 09:30 $1.48 +65.97; FRVO×71 yday $17.16 → 09:30 $17.27 +7.81 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $1,448.77 | ▼ -25.83 after sell → book $10,025.56; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 84 | $15.00 | $2.27 | $-42.31 | $2,706.50 | ▼ -42.31 after sell → book $10,023.29; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $3,933.43 | ▲ +57.35 after sell → book $10,021.26; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 77 | $15.61 | $2.24 | $-93.78 | $5,133.15 | ▼ -93.78 after sell → book $10,019.01; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 23 | $55.79 | $2.08 | $+4.37 | $6,414.24 | ▲ +4.37 after sell → book $10,016.93; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 598 | $2.16 | $7.82 | $-27.50 | $7,698.10 | ▼ -27.50 after sell → book $10,009.11; vs 09:30 mark -7.82 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `GPRO` | 733 | $1.48 | $9.59 | $-238.94 | $8,773.35 | ▼ -238.94 after sell → book $9,999.52; vs 09:30 mark -9.59 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 71 | $17.27 | $2.22 | $-76.14 | $9,997.30 | ▼ -76.14 after sell → book $9,997.30; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 361 | $3.46 | $4.66 | — | $8,743.58 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1249.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 495 | $2.52 | $6.39 | — | $7,489.80 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1249.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 186 | $6.71 | $2.55 | — | $6,239.19 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1249.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 657 | $1.90 | $8.48 | — | $4,982.41 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1249.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 261 | $4.78 | $3.37 | — | $3,731.47 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1249.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 785 | $1.59 | $10.13 | — | $2,473.19 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1249.66 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 110 | $11.31 | $2.32 | — | $1,226.77 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1249.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 347 | $3.52 | $4.48 | — | $0.85 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.3; leftover $1249.66 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.85 | ▲ close $10,041.33 vs 09:30 $10,027.60 (session +86.39) | 16:00 close · cash $0.85 · equity $10,041.33 vs 09:30 $10,027.60 (+13.73; session marks +86.39) · 8 name(s) marked open→close (per-name table). CABA×361 09:30 $3.46 → close $3.47 +3.61; ALEC×495 09:30 $2.52 → close $2.46 -29.70; BHC×186 09:30 $6.71 → close $6.56 -27.90; BMEA×657 09:30 $1.90 → close $2.03 +85.41; OABI×261 09:30 $4.78 → close $4.33 -117.45; OPK×785 09:30 $1.59 → close $1.64 +39.25; VIR×110 09:30 $11.31 → close $11.38 +8.25; EOSE×347 09:30 $3.52 → close $3.88 +124.92 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.85 | ▼ 09:30 equity $9,973.78 vs yday $10,041.33 (-67.55) | 09:30 open · cash $0.85 (unchanged overnight, no fees) · equity $9,973.78 vs prior close $10,041.33 (-67.55) · 8 name(s) re-marked at the open (per-name table). CABA×361 yday $3.47 → 09:30 $3.43 -14.44; ALEC×495 yday $2.46 → 09:30 $2.38 -39.60; BHC×186 yday $6.56 → 09:30 $6.57 +1.86; BMEA×657 yday $2.03 → 09:30 $2.00 -19.71; OABI×261 yday $4.33 → 09:30 $4.30 -7.83; OPK×785 yday $1.64 → 09:30 $1.63 -7.85; VIR×110 yday $11.38 → 09:30 $11.22 -18.15; EOSE×347 yday $3.88 → 09:30 $3.99 +38.17 | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 361 | $3.43 | $4.73 | $-20.21 | $1,234.36 | ▼ -20.21 after sell → book $9,969.06; vs 09:30 mark -4.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 495 | $2.38 | $6.48 | $-82.16 | $2,405.98 | ▼ -82.16 after sell → book $9,962.58; vs 09:30 mark -6.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 186 | $6.57 | $2.59 | $-31.18 | $3,625.41 | ▼ -31.18 after sell → book $9,959.99; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 657 | $2.00 | $8.59 | $+48.63 | $4,930.82 | ▲ +48.63 after sell → book $9,951.40; vs 09:30 mark -8.59 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 261 | $4.30 | $3.42 | $-132.07 | $6,049.70 | ▼ -132.07 after sell → book $9,947.98; vs 09:30 mark -3.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 785 | $1.63 | $10.27 | $+11.01 | $7,318.98 | ▲ +11.01 after sell → book $9,937.71; vs 09:30 mark -10.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 110 | $11.22 | $2.35 | $-14.57 | $8,550.83 | ▼ -14.57 after sell → book $9,935.36; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `EOSE` | 347 | $3.99 | $4.54 | $+154.07 | $9,930.81 | ▲ +154.07 after sell → book $9,930.81; vs 09:30 mark -4.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,930.81 | ▲ close $9,930.81 vs 09:30 $9,973.78 (session +0.00) | 16:00 close · cash $9,930.81 · no lots left · equity $9,930.81. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
