# Factor mine action — `short_news_pack_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · short morning packet news🔴

Cash book **+5.51%** ($10,551) · signal-only (no cash/fees) was +5.19%. Starts YES **12/24**. Fills 22 · skips 22 · realized $+551.27.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only short names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will fall.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the morning news packet box is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a SHORT sleeve: it borrows the name and profits if the price falls. Equity treats the short as a liability (must keep enough to cover).

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news_box=bad` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,551.27.

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
| 2026-08-20 | `AEM` | 8 | — | $204.45 | +0.00 | $212.04 | -60.72 | -60.72 | -0.00 | -60.72 |
| 2026-08-20 | `TEAM` | 9 | — | $173.90 | +0.00 | $174.91 | -9.09 | -9.09 | -0.00 | -9.09 |
| 2026-08-20 | `WMT` | 15 | — | $106.38 | +0.00 | $103.84 | +38.10 | +38.10 | -0.00 | +38.10 |
| 2026-08-21 | `AEM` | 8 | $212.04 | $216.30 | -34.08 | $216.06 | +1.92 | -32.16 | -94.80 | -92.88 |
| 2026-08-21 | `TEAM` | 9 | $174.91 | $174.22 | +6.21 | $171.81 | +21.69 | +27.90 | -2.88 | +18.81 |
| 2026-08-21 | `WMT` | 15 | $103.84 | $103.69 | +2.25 | $103.70 | -0.15 | +2.10 | +40.35 | +40.20 |
| 2026-08-21 | `AUGO` | 27 | — | $89.10 | +0.00 | $87.26 | +49.68 | +49.68 | -0.00 | +49.68 |
| 2026-08-21 | `SSRM` | 64 | — | $38.40 | +0.00 | $37.77 | +40.32 | +40.32 | -0.00 | +40.32 |
| 2026-08-24 | `AEM` | 8 | $216.06 | $217.03 | -7.76 | $217.89 | -6.88 | -14.64 | -100.64 | -107.52 |
| 2026-08-24 | `TEAM` | 9 | $171.81 | $169.30 | +22.59 | $171.33 | -18.27 | +4.32 | +41.40 | +23.13 |
| 2026-08-24 | `WMT` | 15 | $103.70 | $104.14 | -6.60 | $106.49 | -35.25 | -41.85 | +33.60 | -1.65 |
| 2026-08-24 | `AUGO` | 27 | $87.26 | $88.60 | -36.18 | $87.37 | +33.21 | -2.97 | +13.50 | +46.71 |
| 2026-08-24 | `SSRM` | 64 | $37.77 | $38.32 | -35.20 | $38.61 | -18.56 | -53.76 | +5.12 | -13.44 |
| 2026-08-25 | `AEM` | 8 | $217.89 | $212.00 | +47.12 | — | +0.00 | +47.12 | -60.40 | — |
| 2026-08-25 | `TEAM` | 9 | $171.33 | $170.64 | +6.21 | — | +0.00 | +6.21 | +29.34 | — |
| 2026-08-25 | `WMT` | 15 | $106.49 | $105.58 | +13.65 | — | +0.00 | +13.65 | +12.00 | — |
| 2026-08-25 | `AUGO` | 27 | $87.37 | $85.78 | +42.93 | $90.47 | -126.63 | -83.70 | +89.64 | -36.99 |
| 2026-08-25 | `SSRM` | 64 | $38.61 | $37.75 | +55.04 | $39.21 | -93.44 | -38.40 | +41.60 | -51.84 |
| 2026-08-25 | `ARE` | 46 | — | $54.51 | +0.00 | $52.90 | +74.06 | +74.06 | -0.00 | +74.06 |
| 2026-08-25 | `BMO` | 14 | — | $175.01 | +0.00 | $173.46 | +21.70 | +21.70 | -0.00 | +21.70 |
| 2026-08-26 | `AUGO` | 27 | $90.47 | $88.24 | +60.21 | — | +0.00 | +60.21 | +23.22 | — |
| 2026-08-26 | `SSRM` | 64 | $39.21 | $38.41 | +51.20 | — | +0.00 | +51.20 | -0.64 | — |
| 2026-08-26 | `ARE` | 46 | $52.90 | $52.77 | +5.98 | $52.97 | -9.20 | -3.22 | +80.04 | +70.84 |
| 2026-08-26 | `BMO` | 14 | $173.46 | $173.22 | +3.36 | $172.90 | +4.48 | +7.84 | +25.06 | +29.54 |
| 2026-08-26 | `BE` | 11 | — | $213.94 | +0.00 | $218.21 | -46.97 | -46.97 | -0.00 | -46.97 |
| 2026-08-26 | `NEM` | 19 | — | $132.64 | +0.00 | $131.60 | +19.76 | +19.76 | -0.00 | +19.76 |
| 2026-08-27 | `ARE` | 46 | $52.97 | $52.45 | +23.92 | $52.28 | +7.82 | +31.74 | +94.76 | +102.58 |
| 2026-08-27 | `BMO` | 14 | $172.90 | $172.85 | +0.70 | $172.13 | +10.08 | +10.78 | +30.24 | +40.32 |
| 2026-08-27 | `BE` | 11 | $218.21 | $227.10 | -97.79 | $217.83 | +101.97 | +4.18 | -144.76 | -42.79 |
| 2026-08-27 | `NEM` | 19 | $131.60 | $131.02 | +11.02 | $132.29 | -24.13 | -13.11 | +30.78 | +6.65 |
| 2026-08-28 | `ARE` | 46 | $52.28 | $52.49 | -9.66 | — | +0.00 | -9.66 | +92.92 | — |
| 2026-08-28 | `BMO` | 14 | $172.13 | $172.76 | -8.82 | — | +0.00 | -8.82 | +31.50 | — |
| 2026-08-28 | `BE` | 11 | $217.83 | $215.71 | +23.38 | $210.77 | +54.29 | +77.67 | -19.42 | +34.87 |
| 2026-08-28 | `NEM` | 19 | $132.29 | $132.35 | -1.14 | $127.98 | +83.03 | +81.89 | +5.51 | +88.54 |
| 2026-08-28 | `FIG` | 167 | — | $30.18 | +0.00 | $28.82 | +227.12 | +227.12 | -0.00 | +227.12 |
| 2026-08-31 | `BE` | 11 | $210.77 | $208.88 | +20.79 | — | +0.00 | +20.79 | +55.66 | — |
| 2026-08-31 | `NEM` | 19 | $127.98 | $127.45 | +10.07 | — | +0.00 | +10.07 | +98.61 | — |
| 2026-08-31 | `FIG` | 167 | $28.82 | $27.60 | +203.74 | $27.49 | +18.37 | +222.11 | +430.86 | +449.23 |
| 2026-09-01 | `FIG` | 167 | $27.49 | $27.06 | +71.81 | $27.20 | -23.38 | +48.43 | +521.04 | +497.66 |
| 2026-09-02 | `FIG` | 167 | $27.20 | $26.78 | +70.14 | — | +0.00 | +70.14 | +567.80 | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-08 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `QRVO` | 47 | — | $112.83 | +0.00 | $116.65 | -179.31 | -179.31 | -0.00 | -179.31 |
| 2026-09-14 | `QRVO` | 47 | $116.65 | $114.11 | +119.38 | $107.98 | +288.11 | +407.49 | -59.93 | +228.18 |
| 2026-09-15 | `QRVO` | 47 | $107.98 | $108.40 | -19.74 | $118.06 | -454.02 | -473.76 | +208.44 | -245.58 |
| 2026-09-16 | `QRVO` | 47 | $118.06 | $118.18 | -5.64 | — | +0.00 | -5.64 | -251.22 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | -31.71 | AEM, TEAM, WMT | — | $14,790.13 | $9,962.02 | AEM×8, TEAM×9, WMT×15 |
| 2026-08-21 | +3.25 | $14,790.13 | AEM×8, TEAM×9, WMT×15 | $9,936.40 | -25.62 | +113.46 | AUGO, SSRM | — | $19,648.99 | $10,045.42 | AEM×8, TEAM×9, WMT×15, AUGO×27, SSRM×64 |
| 2026-08-24 | -5.17 | $19,648.99 | AEM×8, TEAM×9, WMT×15, AUGO×27, SSRM×64 | $9,982.27 | -63.15 | -45.75 | — | — | $19,648.99 | $9,936.52 | AEM×8, TEAM×9, WMT×15, AUGO×27, SSRM×64 |
| 2026-08-25 | +1.80 | $19,648.99 | AEM×8, TEAM×9, WMT×15, AUGO×27, SSRM×64 | $10,101.47 | +164.95 | -124.31 | ARE, BMO | AEM, TEAM, WMT | $19,780.71 | $9,966.74 | AUGO×27, SSRM×64, ARE×46, BMO×14 |
| 2026-08-26 | +2.02 | $19,780.71 | AUGO×27, SSRM×64, ARE×46, BMO×14 | $10,087.49 | +120.75 | -31.93 | BE, NEM | AUGO, SSRM | $19,804.97 | $10,047.04 | ARE×46, BMO×14, BE×11, NEM×19 |
| 2026-08-27 | — | $19,804.97 | ARE×46, BMO×14, BE×11, NEM×19 | $9,984.89 | -62.15 | +95.74 | — | — | $19,804.97 | $10,080.63 | ARE×46, BMO×14, BE×11, NEM×19 |
| 2026-08-28 | +0.75 | $19,804.97 | ARE×46, BMO×14, BE×11, NEM×19 | $10,084.39 | +3.76 | +364.44 | FIG | ARE, BMO | $20,004.99 | $10,441.96 | BE×11, NEM×19, FIG×167 |
| 2026-08-31 | -5.85 | $20,004.99 | BE×11, NEM×19, FIG×167 | $10,676.56 | +234.60 | +18.37 | — | BE, NEM | $15,281.69 | $10,690.86 | FIG×167 |
| 2026-09-01 | -6.30 | $15,281.69 | FIG×167 | $10,762.67 | +71.81 | -23.38 | — | — | $15,281.69 | $10,739.29 | FIG×167 |
| 2026-09-02 | -3.83 | $15,281.69 | FIG×167 | $10,809.43 | +70.14 | +0.00 | — | FIG | $10,806.94 | $10,806.94 | — |
| 2026-09-03 | -0.90 | $10,806.94 | — | $10,806.94 | +0.00 | +0.00 | — | — | $10,806.94 | $10,806.94 | — |
| 2026-09-04 | +2.25 | $10,806.94 | — | $10,806.94 | +0.00 | +0.00 | — | — | $10,806.94 | $10,806.94 | — |
| 2026-09-08 | -11.47 | $10,806.94 | — | $10,806.94 | +0.00 | +0.00 | — | — | $10,806.94 | $10,806.94 | — |
| 2026-09-09 | -13.95 | $10,806.94 | — | $10,806.94 | +0.00 | +0.00 | — | — | $10,806.94 | $10,806.94 | — |
| 2026-09-10 | -13.28 | $10,806.94 | — | $10,806.94 | +0.00 | +0.00 | — | — | $10,806.94 | $10,806.94 | — |
| 2026-09-11 | +0.50 | $10,806.94 | — | $10,806.94 | +0.00 | -179.31 | QRVO | — | $16,107.86 | $10,625.31 | QRVO×47 |
| 2026-09-14 | -11.00 | $16,107.86 | QRVO×47 | $10,744.69 | +119.38 | +288.11 | — | — | $16,107.86 | $11,032.80 | QRVO×47 |
| 2026-09-15 | -3.84 | $16,107.86 | QRVO×47 | $11,013.06 | -19.74 | -454.02 | — | — | $16,107.86 | $10,559.04 | QRVO×47 |
| 2026-09-16 | +5.30 | $16,107.86 | QRVO×47 | $10,553.40 | -5.64 | +0.00 | — | QRVO | $10,551.27 | $10,551.27 | — |

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
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 8 | $204.45 | $2.08 | — | $11,633.52 | — | short morning packet news🔴; gate news_box=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1666.67 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 9 | $173.90 | $2.08 | — | $13,196.54 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; 🔵; ret5=+12.2; leftover $1666.67 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 15 | $106.38 | $2.10 | — | $14,790.13 | — | short morning packet news🔴; gate news_box=bad; list earn_react; 🔵; ret5=-1.7; leftover $1666.67 | join🟢 sector🟡 gen🟢 news🔴 digest🟡 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,790.13 | ▼ close $9,962.02 vs 09:30 $10,000.00 (session -31.71) | 16:00 close · cash $14,790.13 · equity $9,962.02 vs 09:30 $10,000.00 (-37.98; session marks -31.71) · 3 name(s) marked open→close (per-name table). AEM×8 09:30 $204.45 → close $212.04 -60.72; TEAM×9 09:30 $173.90 → close $174.91 -9.09; WMT×15 09:30 $106.38 → close $103.84 +38.10 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,790.13 | ▼ 09:30 equity $9,936.40 vs yday $9,962.02 (-25.62) | 09:30 open · cash $14,790.13 (unchanged overnight, no fees) · equity $9,936.40 vs prior close $9,962.02 (-25.62) · 3 name(s) re-marked at the open (per-name table). AEM×8 yday $212.04 → 09:30 $216.30 -34.08; TEAM×9 yday $174.91 → 09:30 $174.22 +6.21; WMT×15 yday $103.84 → 09:30 $103.69 +2.25 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 27 | $89.10 | $2.17 | — | $17,193.67 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $2484.10 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 64 | $38.40 | $2.28 | — | $19,648.99 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $2484.10 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,648.99 | ▲ close $10,045.42 vs 09:30 $9,936.40 (session +113.46) | 16:00 close · cash $19,648.99 · equity $10,045.42 vs 09:30 $9,936.40 (+109.02; session marks +113.46) · 5 name(s) marked open→close (per-name table). AEM×8 09:30 $216.30 → close $216.06 +1.92; TEAM×9 09:30 $174.22 → close $171.81 +21.69; WMT×15 09:30 $103.69 → close $103.70 -0.15; AUGO×27 09:30 $89.10 → close $87.26 +49.68; SSRM×64 09:30 $38.40 → close $37.77 +40.32 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,648.99 | ▼ 09:30 equity $9,982.27 vs yday $10,045.42 (-63.15) | 09:30 open · cash $19,648.99 (unchanged overnight, no fees) · equity $9,982.27 vs prior close $10,045.42 (-63.15) · 5 name(s) re-marked at the open (per-name table). AEM×8 yday $216.06 → 09:30 $217.03 -7.76; TEAM×9 yday $171.81 → 09:30 $169.30 +22.59; WMT×15 yday $103.70 → 09:30 $104.14 -6.60; AUGO×27 yday $87.26 → 09:30 $88.60 -36.18; SSRM×64 yday $37.77 → 09:30 $38.32 -35.20 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,648.99 | ▼ close $9,936.52 vs 09:30 $9,982.27 (session -45.75) | 16:00 close · cash $19,648.99 · equity $9,936.52 vs 09:30 $9,982.27 (-45.75; session marks -45.75) · 5 name(s) marked open→close (per-name table). AEM×8 09:30 $217.03 → close $217.89 -6.88; TEAM×9 09:30 $169.30 → close $171.33 -18.27; WMT×15 09:30 $104.14 → close $106.49 -35.25; AUGO×27 09:30 $88.60 → close $87.37 +33.21; SSRM×64 09:30 $38.32 → close $38.61 -18.56 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,648.99 | ▲ 09:30 equity $10,101.47 vs yday $9,936.52 (+164.95) | 09:30 open · cash $19,648.99 (unchanged overnight, no fees) · equity $10,101.47 vs prior close $9,936.52 (+164.95) · 5 name(s) re-marked at the open (per-name table). AEM×8 yday $217.89 → 09:30 $212.00 +47.12; TEAM×9 yday $171.33 → 09:30 $170.64 +6.21; WMT×15 yday $106.49 → 09:30 $105.58 +13.65; AUGO×27 yday $87.37 → 09:30 $85.78 +42.93; SSRM×64 yday $38.61 → 09:30 $37.75 +55.04 | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 8 | $212.00 | $2.01 | $-64.50 | $17,950.97 | ▼ -64.50 after sell → book $10,099.45; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 9 | $170.64 | $2.02 | $+25.24 | $16,413.20 | ▲ +25.24 after sell → book $10,097.44; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 15 | $105.58 | $2.04 | $+7.86 | $14,827.46 | ▲ +7.86 after sell → book $10,095.40; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 46 | $54.51 | $2.23 | — | $17,332.70 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; ret5=+15.1; leftover $2523.85 | join🔴 sector🟡 gen🟡 news🔴 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 14 | $175.01 | $2.13 | — | $19,780.71 | — | short morning packet news🔴; gate news_box=bad; list earn_react; ret5=-7.0; leftover $2523.85 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,780.71 | ▼ close $9,966.74 vs 09:30 $10,101.47 (session -124.31) | 16:00 close · cash $19,780.71 · equity $9,966.74 vs 09:30 $10,101.47 (-134.73; session marks -124.31) · 4 name(s) marked open→close (per-name table). AUGO×27 09:30 $85.78 → close $90.47 -126.63; SSRM×64 09:30 $37.75 → close $39.21 -93.44; ARE×46 09:30 $54.51 → close $52.90 +74.06; BMO×14 09:30 $175.01 → close $173.46 +21.70 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,780.71 | ▲ 09:30 equity $10,087.49 vs yday $9,966.74 (+120.75) | 09:30 open · cash $19,780.71 (unchanged overnight, no fees) · equity $10,087.49 vs prior close $9,966.74 (+120.75) · 4 name(s) re-marked at the open (per-name table). AUGO×27 yday $90.47 → 09:30 $88.24 +60.21; SSRM×64 yday $39.21 → 09:30 $38.41 +51.20; ARE×46 yday $52.90 → 09:30 $52.77 +5.98; BMO×14 yday $173.46 → 09:30 $173.22 +3.36 | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 27 | $88.24 | $2.07 | $+18.98 | $17,396.16 | ▲ +18.98 after sell → book $10,085.42; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 64 | $38.41 | $2.18 | $-5.10 | $14,935.73 | ▼ -5.10 after sell → book $10,083.23; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 11 | $213.94 | $2.12 | — | $17,286.96 | — | short morning packet news🔴; gate news_box=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $2520.81 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 19 | $132.64 | $2.15 | — | $19,804.97 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; 🔵; ret5=+16.5; leftover $2520.81 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,804.97 | ▼ close $10,047.04 vs 09:30 $10,087.49 (session -31.93) | 16:00 close · cash $19,804.97 · equity $10,047.04 vs 09:30 $10,087.49 (-40.45; session marks -31.93) · 4 name(s) marked open→close (per-name table). ARE×46 09:30 $52.77 → close $52.97 -9.20; BMO×14 09:30 $173.22 → close $172.90 +4.48; BE×11 09:30 $213.94 → close $218.21 -46.97; NEM×19 09:30 $132.64 → close $131.60 +19.76 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,804.97 | ▼ 09:30 equity $9,984.89 vs yday $10,047.04 (-62.15) | 09:30 open · cash $19,804.97 (unchanged overnight, no fees) · equity $9,984.89 vs prior close $10,047.04 (-62.15) · 4 name(s) re-marked at the open (per-name table). ARE×46 yday $52.97 → 09:30 $52.45 +23.92; BMO×14 yday $172.90 → 09:30 $172.85 +0.70; BE×11 yday $218.21 → 09:30 $227.10 -97.79; NEM×19 yday $131.60 → 09:30 $131.02 +11.02 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,804.97 | ▲ close $10,080.63 vs 09:30 $9,984.89 (session +95.74) | 16:00 close · cash $19,804.97 · equity $10,080.63 vs 09:30 $9,984.89 (+95.74; session marks +95.74) · 4 name(s) marked open→close (per-name table). ARE×46 09:30 $52.45 → close $52.28 +7.82; BMO×14 09:30 $172.85 → close $172.13 +10.08; BE×11 09:30 $227.10 → close $217.83 +101.97; NEM×19 09:30 $131.02 → close $132.29 -24.13 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,804.97 | ▲ 09:30 equity $10,084.39 vs yday $10,080.63 (+3.76) | 09:30 open · cash $19,804.97 (unchanged overnight, no fees) · equity $10,084.39 vs prior close $10,080.63 (+3.76) · 4 name(s) re-marked at the open (per-name table). ARE×46 yday $52.28 → 09:30 $52.49 -9.66; BMO×14 yday $172.13 → 09:30 $172.76 -8.82; BE×11 yday $217.83 → 09:30 $215.71 +23.38; NEM×19 yday $132.29 → 09:30 $132.35 -1.14 | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 46 | $52.49 | $2.13 | $+88.57 | $17,388.30 | ▲ +88.57 after sell → book $10,082.26; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 14 | $172.76 | $2.03 | $+27.34 | $14,967.63 | ▲ +27.34 after sell → book $10,080.23; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 167 | $30.18 | $2.70 | — | $20,004.99 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; ret5=+12.1; leftover $5040.11 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20,004.99 | ▲ close $10,441.96 vs 09:30 $10,084.39 (session +364.44) | 16:00 close · cash $20,004.99 · equity $10,441.96 vs 09:30 $10,084.39 (+357.57; session marks +364.44) · 3 name(s) marked open→close (per-name table). BE×11 09:30 $215.71 → close $210.77 +54.29; NEM×19 09:30 $132.35 → close $127.98 +83.03; FIG×167 09:30 $30.18 → close $28.82 +227.12 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,004.99 | ▲ 09:30 equity $10,676.56 vs yday $10,441.96 (+234.60) | 09:30 open · cash $20,004.99 (unchanged overnight, no fees) · equity $10,676.56 vs prior close $10,441.96 (+234.60) · 3 name(s) re-marked at the open (per-name table). BE×11 yday $210.77 → 09:30 $208.88 +20.79; NEM×19 yday $127.98 → 09:30 $127.45 +10.07; FIG×167 yday $28.82 → 09:30 $27.60 +203.74 | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 11 | $208.88 | $2.02 | $+51.52 | $17,705.29 | ▲ +51.52 after sell → book $10,674.54; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 19 | $127.45 | $2.05 | $+94.42 | $15,281.69 | ▲ +94.42 after sell → book $10,672.49; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,281.69 | ▲ close $10,690.86 vs 09:30 $10,676.56 (session +18.37) | 16:00 close · cash $15,281.69 · equity $10,690.86 vs 09:30 $10,676.56 (+14.30; session marks +18.37) · 1 name(s) marked open→close (per-name table). FIG×167 09:30 $27.60 → close $27.49 +18.37 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,281.69 | ▲ 09:30 equity $10,762.67 vs yday $10,690.86 (+71.81) | 09:30 open · cash $15,281.69 (unchanged overnight, no fees) · equity $10,762.67 vs prior close $10,690.86 (+71.81) · 1 name(s) re-marked at the open (per-name table). FIG×167 yday $27.49 → 09:30 $27.06 +71.81 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,281.69 | ▼ close $10,739.29 vs 09:30 $10,762.67 (session -23.38) | 16:00 close · cash $15,281.69 · equity $10,739.29 vs 09:30 $10,762.67 (-23.38; session marks -23.38) · 1 name(s) marked open→close (per-name table). FIG×167 09:30 $27.06 → close $27.20 -23.38 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,281.69 | ▲ 09:30 equity $10,809.43 vs yday $10,739.29 (+70.14) | 09:30 open · cash $15,281.69 (unchanged overnight, no fees) · equity $10,809.43 vs prior close $10,739.29 (+70.14) · 1 name(s) re-marked at the open (per-name table). FIG×167 yday $27.20 → 09:30 $26.78 +70.14 | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 167 | $26.78 | $2.49 | $+562.61 | $10,806.94 | ▲ +562.61 after sell → book $10,806.94; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.94 | ▲ close $10,806.94 vs 09:30 $10,809.43 (session +0.00) | 16:00 close · cash $10,806.94 · no lots left · equity $10,806.94. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.94 | ▲ 09:30 equity $10,806.94 vs yday $10,806.94 (+0.00) | 09:30 open · cash $10,806.94 · no holdings · equity $10,806.94 vs prior close $10,806.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.94 | ▲ close $10,806.94 vs 09:30 $10,806.94 (session +0.00) | 16:00 close · cash $10,806.94 · no lots left · equity $10,806.94. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.94 | ▲ 09:30 equity $10,806.94 vs yday $10,806.94 (+0.00) | 09:30 open · cash $10,806.94 · no holdings · equity $10,806.94 vs prior close $10,806.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.94 | ▲ close $10,806.94 vs 09:30 $10,806.94 (session +0.00) | 16:00 close · cash $10,806.94 · no lots left · equity $10,806.94. | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.94 | ▲ 09:30 equity $10,806.94 vs yday $10,806.94 (+0.00) | 09:30 open · cash $10,806.94 · no holdings · equity $10,806.94 vs prior close $10,806.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.94 | ▲ close $10,806.94 vs 09:30 $10,806.94 (session +0.00) | 16:00 close · cash $10,806.94 · no lots left · equity $10,806.94. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.94 | ▲ 09:30 equity $10,806.94 vs yday $10,806.94 (+0.00) | 09:30 open · cash $10,806.94 · no holdings · equity $10,806.94 vs prior close $10,806.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.94 | ▲ close $10,806.94 vs 09:30 $10,806.94 (session +0.00) | 16:00 close · cash $10,806.94 · no lots left · equity $10,806.94. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.94 | ▲ 09:30 equity $10,806.94 vs yday $10,806.94 (+0.00) | 09:30 open · cash $10,806.94 · no holdings · equity $10,806.94 vs prior close $10,806.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,806.94 | ▲ close $10,806.94 vs 09:30 $10,806.94 (session +0.00) | 16:00 close · cash $10,806.94 · no lots left · equity $10,806.94. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,806.94 | ▲ 09:30 equity $10,806.94 vs yday $10,806.94 (+0.00) | 09:30 open · cash $10,806.94 · no holdings · equity $10,806.94 vs prior close $10,806.94 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 47 | $112.83 | $2.33 | — | $16,107.86 | — | short morning packet news🔴; gate news_box=bad; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $5403.47 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,107.86 | ▼ close $10,625.31 vs 09:30 $10,806.94 (session -179.31) | 16:00 close · cash $16,107.86 · equity $10,625.31 vs 09:30 $10,806.94 (-181.63; session marks -179.31) · 1 name(s) marked open→close (per-name table). QRVO×47 09:30 $112.83 → close $116.65 -179.31 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,107.86 | ▲ 09:30 equity $10,744.69 vs yday $10,625.31 (+119.38) | 09:30 open · cash $16,107.86 (unchanged overnight, no fees) · equity $10,744.69 vs prior close $10,625.31 (+119.38) · 1 name(s) re-marked at the open (per-name table). QRVO×47 yday $116.65 → 09:30 $114.11 +119.38 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,107.86 | ▲ close $11,032.80 vs 09:30 $10,744.69 (session +288.11) | 16:00 close · cash $16,107.86 · equity $11,032.80 vs 09:30 $10,744.69 (+288.11; session marks +288.11) · 1 name(s) marked open→close (per-name table). QRVO×47 09:30 $114.11 → close $107.98 +288.11 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,107.86 | ▼ 09:30 equity $11,013.06 vs yday $11,032.80 (-19.74) | 09:30 open · cash $16,107.86 (unchanged overnight, no fees) · equity $11,013.06 vs prior close $11,032.80 (-19.74) · 1 name(s) re-marked at the open (per-name table). QRVO×47 yday $107.98 → 09:30 $108.40 -19.74 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,107.86 | ▼ close $10,559.04 vs 09:30 $11,013.06 (session -454.02) | 16:00 close · cash $16,107.86 · equity $10,559.04 vs 09:30 $11,013.06 (-454.02; session marks -454.02) · 1 name(s) marked open→close (per-name table). QRVO×47 09:30 $108.40 → close $118.06 -454.02 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16,107.86 | ▼ 09:30 equity $10,553.40 vs yday $10,559.04 (-5.64) | 09:30 open · cash $16,107.86 (unchanged overnight, no fees) · equity $10,553.40 vs prior close $10,559.04 (-5.64) · 1 name(s) re-marked at the open (per-name table). QRVO×47 yday $118.06 → 09:30 $118.18 -5.64 | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 47 | $118.18 | $2.13 | $-255.67 | $10,551.27 | ▼ -255.67 after sell → book $10,551.27; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,551.27 | ▲ close $10,551.27 vs 09:30 $10,553.40 (session +0.00) | 16:00 close · cash $10,551.27 · no lots left · equity $10,551.27. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-21 | `AEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WMT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TEAM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WMT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SSRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `AUGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `NEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `FIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `FIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-14 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
