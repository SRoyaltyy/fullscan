# Factor mine action — `short_news_pack_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · short morning packet news🔴

Cash book **+0.61%** ($10,061) · signal-only (no cash/fees) was -6.15%. Starts YES **9/28**. Fills 26 · skips 26 · realized $+60.75.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,060.77.

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
| 2026-08-25 | `ARE` | 30 | — | $54.51 | +0.00 | $52.90 | +48.30 | +48.30 | -0.00 | +48.30 |
| 2026-08-25 | `BMO` | 9 | — | $175.01 | +0.00 | $173.46 | +13.95 | +13.95 | -0.00 | +13.95 |
| 2026-08-25 | `INTU` | 4 | — | $364.35 | +0.00 | $357.46 | +27.56 | +27.56 | -0.00 | +27.56 |
| 2026-08-26 | `AUGO` | 27 | $90.47 | $88.24 | +60.21 | — | +0.00 | +60.21 | +23.22 | — |
| 2026-08-26 | `SSRM` | 64 | $39.21 | $38.41 | +51.20 | — | +0.00 | +51.20 | -0.64 | — |
| 2026-08-26 | `ARE` | 30 | $52.90 | $52.77 | +3.90 | $52.97 | -6.00 | -2.10 | +52.20 | +46.20 |
| 2026-08-26 | `BMO` | 9 | $173.46 | $173.22 | +2.16 | $172.90 | +2.88 | +5.04 | +16.11 | +18.99 |
| 2026-08-26 | `INTU` | 4 | $357.46 | $323.47 | +135.96 | $345.88 | -89.64 | +46.32 | +163.52 | +73.88 |
| 2026-08-26 | `BE` | 7 | — | $213.94 | +0.00 | $218.21 | -29.89 | -29.89 | -0.00 | -29.89 |
| 2026-08-26 | `NEM` | 12 | — | $132.64 | +0.00 | $131.60 | +12.48 | +12.48 | -0.00 | +12.48 |
| 2026-08-26 | `CRM` | 8 | — | $199.94 | +0.00 | $205.62 | -45.44 | -45.44 | -0.00 | -45.44 |
| 2026-08-27 | `ARE` | 30 | $52.97 | $52.45 | +15.60 | $52.28 | +5.10 | +20.70 | +61.80 | +66.90 |
| 2026-08-27 | `BMO` | 9 | $172.90 | $172.85 | +0.45 | $172.13 | +6.48 | +6.93 | +19.44 | +25.92 |
| 2026-08-27 | `INTU` | 4 | $345.88 | $353.54 | -30.64 | $348.00 | +22.16 | -8.48 | +43.24 | +65.40 |
| 2026-08-27 | `BE` | 7 | $218.21 | $227.10 | -62.23 | $217.83 | +64.89 | +2.66 | -92.12 | -27.23 |
| 2026-08-27 | `NEM` | 12 | $131.60 | $131.02 | +6.96 | $132.29 | -15.24 | -8.28 | +19.44 | +4.20 |
| 2026-08-27 | `CRM` | 8 | $205.62 | $230.05 | -195.44 | $252.05 | -176.00 | -371.44 | -240.88 | -416.88 |
| 2026-08-28 | `ARE` | 30 | $52.28 | $52.49 | -6.30 | — | +0.00 | -6.30 | +60.60 | — |
| 2026-08-28 | `BMO` | 9 | $172.13 | $172.76 | -5.67 | — | +0.00 | -5.67 | +20.25 | — |
| 2026-08-28 | `INTU` | 4 | $348.00 | $347.82 | +0.72 | — | +0.00 | +0.72 | +66.12 | — |
| 2026-08-28 | `BE` | 7 | $217.83 | $215.71 | +14.88 | $210.77 | +34.55 | +49.43 | -12.36 | +22.19 |
| 2026-08-28 | `NEM` | 12 | $132.29 | $132.35 | -0.72 | $127.98 | +52.44 | +51.72 | +3.48 | +55.92 |
| 2026-08-28 | `CRM` | 8 | $252.05 | $250.47 | +12.64 | $256.00 | -44.24 | -31.60 | -404.24 | -448.48 |
| 2026-08-28 | `FIG` | 160 | — | $30.18 | +0.00 | $28.82 | +217.60 | +217.60 | -0.00 | +217.60 |
| 2026-08-31 | `BE` | 7 | $210.77 | $208.88 | +13.23 | — | +0.00 | +13.23 | +35.42 | — |
| 2026-08-31 | `NEM` | 12 | $127.98 | $127.45 | +6.36 | — | +0.00 | +6.36 | +62.28 | — |
| 2026-08-31 | `CRM` | 8 | $256.00 | $254.39 | +12.88 | — | +0.00 | +12.88 | -435.60 | — |
| 2026-08-31 | `FIG` | 160 | $28.82 | $27.60 | +195.20 | $27.49 | +17.60 | +212.80 | +412.80 | +430.40 |
| 2026-09-01 | `FIG` | 160 | $27.49 | $27.06 | +68.80 | $27.20 | -22.40 | +46.40 | +499.20 | +476.80 |
| 2026-09-02 | `FIG` | 160 | $27.20 | $26.78 | +67.20 | — | +0.00 | +67.20 | +544.00 | — |
| 2026-09-03 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-04 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-08 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `QRVO` | 45 | — | $112.83 | +0.00 | $116.65 | -171.68 | -171.68 | -0.00 | -171.68 |
| 2026-09-14 | `QRVO` | 45 | $116.65 | $114.11 | +114.30 | $107.98 | +275.85 | +390.15 | -57.38 | +218.47 |
| 2026-09-15 | `QRVO` | 45 | $107.98 | $108.40 | -18.90 | $118.06 | -434.70 | -453.60 | +199.57 | -235.13 |
| 2026-09-16 | `QRVO` | 45 | $118.06 | $118.18 | -5.40 | — | +0.00 | -5.40 | -240.53 | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-21 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-22 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

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
| 2026-08-25 | +1.80 | $19,648.99 | AEM×8, TEAM×9, WMT×15, AUGO×27, SSRM×64 | $10,101.47 | +164.95 | -130.26 | ARE, BMO, INTU | AEM, TEAM, WMT | $19,488.96 | $9,958.85 | AUGO×27, SSRM×64, ARE×30, BMO×9, INTU×4 |
| 2026-08-26 | +2.02 | $19,488.96 | AUGO×27, SSRM×64, ARE×30, BMO×9, INTU×4 | $10,212.28 | +253.43 | -155.61 | BE, NEM, CRM | AUGO, SSRM | $19,326.52 | $10,046.17 | ARE×30, BMO×9, INTU×4, BE×7, NEM×12, CRM×8 |
| 2026-08-27 | — | $19,326.52 | ARE×30, BMO×9, INTU×4, BE×7, NEM×12, CRM×8 | $9,780.87 | -265.30 | -92.61 | — | — | $19,326.52 | $9,688.26 | ARE×30, BMO×9, INTU×4, BE×7, NEM×12, CRM×8 |
| 2026-08-28 | +0.75 | $19,326.52 | ARE×30, BMO×9, INTU×4, BE×7, NEM×12, CRM×8 | $9,703.80 | +15.54 | +260.35 | FIG | ARE, BMO, INTU | $19,625.73 | $9,955.38 | BE×7, NEM×12, CRM×8, FIG×160 |
| 2026-08-31 | -5.85 | $19,625.73 | BE×7, NEM×12, CRM×8, FIG×160 | $10,183.05 | +227.67 | +17.60 | — | BE, NEM, CRM | $14,593.00 | $10,194.60 | FIG×160 |
| 2026-09-01 | -6.30 | $14,593.00 | FIG×160 | $10,263.40 | +68.80 | -22.40 | — | — | $14,593.00 | $10,241.00 | FIG×160 |
| 2026-09-02 | -3.83 | $14,593.00 | FIG×160 | $10,308.20 | +67.20 | +0.00 | — | FIG | $10,305.73 | $10,305.73 | — |
| 2026-09-03 | -0.90 | $10,305.73 | — | $10,305.73 | +0.00 | +0.00 | — | — | $10,305.73 | $10,305.73 | — |
| 2026-09-04 | +2.25 | $10,305.73 | — | $10,305.73 | +0.00 | +0.00 | — | — | $10,305.73 | $10,305.73 | — |
| 2026-09-08 | -11.47 | $10,305.73 | — | $10,305.73 | +0.00 | +0.00 | — | — | $10,305.73 | $10,305.73 | — |
| 2026-09-09 | -13.95 | $10,305.73 | — | $10,305.73 | +0.00 | +0.00 | — | — | $10,305.73 | $10,305.73 | — |
| 2026-09-10 | -13.28 | $10,305.73 | — | $10,305.73 | +0.00 | +0.00 | — | — | $10,305.73 | $10,305.73 | — |
| 2026-09-11 | +0.50 | $10,305.73 | — | $10,305.73 | +0.00 | -171.68 | QRVO | — | $15,380.99 | $10,131.74 | QRVO×45 |
| 2026-09-14 | -11.00 | $15,380.99 | QRVO×45 | $10,246.04 | +114.30 | +275.85 | — | — | $15,380.99 | $10,521.89 | QRVO×45 |
| 2026-09-15 | -3.84 | $15,380.99 | QRVO×45 | $10,502.99 | -18.90 | -434.70 | — | — | $15,380.99 | $10,068.29 | QRVO×45 |
| 2026-09-16 | +5.30 | $15,380.99 | QRVO×45 | $10,062.89 | -5.40 | +0.00 | — | QRVO | $10,060.77 | $10,060.77 | — |
| 2026-09-17 | +7.38 | $10,060.77 | — | $10,060.77 | -0.00 | +0.00 | — | — | $10,060.77 | $10,060.77 | — |
| 2026-09-18 | +4.86 | $10,060.77 | — | $10,060.77 | -0.00 | +0.00 | — | — | $10,060.77 | $10,060.77 | — |
| 2026-09-21 | +12.87 | $10,060.77 | — | $10,060.77 | -0.00 | +0.00 | — | — | $10,060.77 | $10,060.77 | — |
| 2026-09-22 | -0.50 | $10,060.77 | — | $10,060.77 | -0.00 | +0.00 | — | — | $10,060.77 | $10,060.77 | — |

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
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 30 | $54.51 | $2.15 | — | $16,460.61 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; ret5=+15.1; leftover $1682.57 | join🔴 sector🟡 gen🟡 news🔴 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 9 | $175.01 | $2.08 | — | $18,033.62 | — | short morning packet news🔴; gate news_box=bad; list earn_react; ret5=-7.0; leftover $1682.57 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 4 | $364.35 | $2.06 | — | $19,488.96 | — | short morning packet news🔴; gate news_box=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $1682.57 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 judge🟡 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,488.96 | ▼ close $9,958.85 vs 09:30 $10,101.47 (session -130.26) | 16:00 close · cash $19,488.96 · equity $9,958.85 vs 09:30 $10,101.47 (-142.62; session marks -130.26) · 5 name(s) marked open→close (per-name table). AUGO×27 09:30 $85.78 → close $90.47 -126.63; SSRM×64 09:30 $37.75 → close $39.21 -93.44; ARE×30 09:30 $54.51 → close $52.90 +48.30; BMO×9 09:30 $175.01 → close $173.46 +13.95; INTU×4 09:30 $364.35 → close $357.46 +27.56 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,488.96 | ▲ 09:30 equity $10,212.28 vs yday $9,958.85 (+253.43) | 09:30 open · cash $19,488.96 (unchanged overnight, no fees) · equity $10,212.28 vs prior close $9,958.85 (+253.43) · 5 name(s) re-marked at the open (per-name table). AUGO×27 yday $90.47 → 09:30 $88.24 +60.21; SSRM×64 yday $39.21 → 09:30 $38.41 +51.20; ARE×30 yday $52.90 → 09:30 $52.77 +3.90; BMO×9 yday $173.46 → 09:30 $173.22 +2.16; INTU×4 yday $357.46 → 09:30 $323.47 +135.96 | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 27 | $88.24 | $2.07 | $+18.98 | $17,104.41 | ▲ +18.98 after sell → book $10,210.21; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 64 | $38.41 | $2.18 | $-5.10 | $14,643.99 | ▼ -5.10 after sell → book $10,208.03; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 7 | $213.94 | $2.07 | — | $16,139.49 | — | short morning packet news🔴; gate news_box=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1701.34 | join🟢 sector🔴 gen🟢 news🔴 digest🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 12 | $132.64 | $2.09 | — | $17,729.08 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; 🔵; ret5=+16.5; leftover $1701.34 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 8 | $199.94 | $2.08 | — | $19,326.52 | — | short morning packet news🔴; gate news_box=bad; list overnight,overnight_mega; ret5=+2.1; leftover $1701.34 | join🟡 sector🔴 gen🟢 news🔴 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 catal🟡 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,326.52 | ▼ close $10,046.17 vs 09:30 $10,212.28 (session -155.61) | 16:00 close · cash $19,326.52 · equity $10,046.17 vs 09:30 $10,212.28 (-166.11; session marks -155.61) · 6 name(s) marked open→close (per-name table). ARE×30 09:30 $52.77 → close $52.97 -6.00; BMO×9 09:30 $173.22 → close $172.90 +2.88; INTU×4 09:30 $323.47 → close $345.88 -89.64; BE×7 09:30 $213.94 → close $218.21 -29.89; NEM×12 09:30 $132.64 → close $131.60 +12.48; CRM×8 09:30 $199.94 → close $205.62 -45.44 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,326.52 | ▼ 09:30 equity $9,780.87 vs yday $10,046.17 (-265.30) | 09:30 open · cash $19,326.52 (unchanged overnight, no fees) · equity $9,780.87 vs prior close $10,046.17 (-265.30) · 6 name(s) re-marked at the open (per-name table). ARE×30 yday $52.97 → 09:30 $52.45 +15.60; BMO×9 yday $172.90 → 09:30 $172.85 +0.45; INTU×4 yday $345.88 → 09:30 $353.54 -30.64; BE×7 yday $218.21 → 09:30 $227.10 -62.23; NEM×12 yday $131.60 → 09:30 $131.02 +6.96; CRM×8 yday $205.62 → 09:30 $230.05 -195.44 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,326.52 | ▼ close $9,688.26 vs 09:30 $9,780.87 (session -92.61) | 16:00 close · cash $19,326.52 · equity $9,688.26 vs 09:30 $9,780.87 (-92.61; session marks -92.61) · 6 name(s) marked open→close (per-name table). ARE×30 09:30 $52.45 → close $52.28 +5.10; BMO×9 09:30 $172.85 → close $172.13 +6.48; INTU×4 09:30 $353.54 → close $348.00 +22.16; BE×7 09:30 $227.10 → close $217.83 +64.89; NEM×12 09:30 $131.02 → close $132.29 -15.24; CRM×8 09:30 $230.05 → close $252.05 -176.00 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,326.52 | ▲ 09:30 equity $9,703.80 vs yday $9,688.26 (+15.54) | 09:30 open · cash $19,326.52 (unchanged overnight, no fees) · equity $9,703.80 vs prior close $9,688.26 (+15.54) · 6 name(s) re-marked at the open (per-name table). ARE×30 yday $52.28 → 09:30 $52.49 -6.30; BMO×9 yday $172.13 → 09:30 $172.76 -5.67; INTU×4 yday $348.00 → 09:30 $347.82 +0.72; BE×7 yday $217.83 → 09:30 $215.71 +14.88; NEM×12 yday $132.29 → 09:30 $132.35 -0.72; CRM×8 yday $252.05 → 09:30 $250.47 +12.64 | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 30 | $52.49 | $2.08 | $+56.37 | $17,749.74 | ▲ +56.37 after sell → book $9,701.72; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 9 | $172.76 | $2.02 | $+16.15 | $16,192.88 | ▲ +16.15 after sell → book $9,699.71; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 4 | $347.82 | $2.00 | $+62.05 | $14,799.60 | ▲ +62.05 after sell → book $9,697.70; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 160 | $30.18 | $2.67 | — | $19,625.73 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; ret5=+12.1; leftover $4848.85 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,625.73 | ▲ close $9,955.38 vs 09:30 $9,703.80 (session +260.35) | 16:00 close · cash $19,625.73 · equity $9,955.38 vs 09:30 $9,703.80 (+251.58; session marks +260.35) · 4 name(s) marked open→close (per-name table). BE×7 09:30 $215.71 → close $210.77 +34.55; NEM×12 09:30 $132.35 → close $127.98 +52.44; CRM×8 09:30 $250.47 → close $256.00 -44.24; FIG×160 09:30 $30.18 → close $28.82 +217.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,625.73 | ▲ 09:30 equity $10,183.05 vs yday $9,955.38 (+227.67) | 09:30 open · cash $19,625.73 (unchanged overnight, no fees) · equity $10,183.05 vs prior close $9,955.38 (+227.67) · 4 name(s) re-marked at the open (per-name table). BE×7 yday $210.77 → 09:30 $208.88 +13.23; NEM×12 yday $127.98 → 09:30 $127.45 +6.36; CRM×8 yday $256.00 → 09:30 $254.39 +12.88; FIG×160 yday $28.82 → 09:30 $27.60 +195.20 | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 7 | $208.88 | $2.01 | $+31.33 | $18,161.56 | ▲ +31.33 after sell → book $10,181.04; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 12 | $127.45 | $2.03 | $+58.16 | $16,630.13 | ▲ +58.16 after sell → book $10,179.01; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 8 | $254.39 | $2.01 | $-439.69 | $14,593.00 | ▼ -439.69 after sell → book $10,177.00; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,593.00 | ▲ close $10,194.60 vs 09:30 $10,183.05 (session +17.60) | 16:00 close · cash $14,593.00 · equity $10,194.60 vs 09:30 $10,183.05 (+11.55; session marks +17.60) · 1 name(s) marked open→close (per-name table). FIG×160 09:30 $27.60 → close $27.49 +17.60 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,593.00 | ▲ 09:30 equity $10,263.40 vs yday $10,194.60 (+68.80) | 09:30 open · cash $14,593.00 (unchanged overnight, no fees) · equity $10,263.40 vs prior close $10,194.60 (+68.80) · 1 name(s) re-marked at the open (per-name table). FIG×160 yday $27.49 → 09:30 $27.06 +68.80 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,593.00 | ▼ close $10,241.00 vs 09:30 $10,263.40 (session -22.40) | 16:00 close · cash $14,593.00 · equity $10,241.00 vs 09:30 $10,263.40 (-22.40; session marks -22.40) · 1 name(s) marked open→close (per-name table). FIG×160 09:30 $27.06 → close $27.20 -22.40 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,593.00 | ▲ 09:30 equity $10,308.20 vs yday $10,241.00 (+67.20) | 09:30 open · cash $14,593.00 (unchanged overnight, no fees) · equity $10,308.20 vs prior close $10,241.00 (+67.20) · 1 name(s) re-marked at the open (per-name table). FIG×160 yday $27.20 → 09:30 $26.78 +67.20 | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 160 | $26.78 | $2.47 | $+538.86 | $10,305.73 | ▲ +538.86 after sell → book $10,305.73; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,305.73 | ▲ close $10,305.73 vs 09:30 $10,308.20 (session +0.00) | 16:00 close · cash $10,305.73 · no lots left · equity $10,305.73. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,305.73 | ▲ 09:30 equity $10,305.73 vs yday $10,305.73 (+0.00) | 09:30 open · cash $10,305.73 · no holdings · equity $10,305.73 vs prior close $10,305.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,305.73 | ▲ close $10,305.73 vs 09:30 $10,305.73 (session +0.00) | 16:00 close · cash $10,305.73 · no lots left · equity $10,305.73. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,305.73 | ▲ 09:30 equity $10,305.73 vs yday $10,305.73 (+0.00) | 09:30 open · cash $10,305.73 · no holdings · equity $10,305.73 vs prior close $10,305.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,305.73 | ▲ close $10,305.73 vs 09:30 $10,305.73 (session +0.00) | 16:00 close · cash $10,305.73 · no lots left · equity $10,305.73. | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,305.73 | ▲ 09:30 equity $10,305.73 vs yday $10,305.73 (+0.00) | 09:30 open · cash $10,305.73 · no holdings · equity $10,305.73 vs prior close $10,305.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,305.73 | ▲ close $10,305.73 vs 09:30 $10,305.73 (session +0.00) | 16:00 close · cash $10,305.73 · no lots left · equity $10,305.73. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,305.73 | ▲ 09:30 equity $10,305.73 vs yday $10,305.73 (+0.00) | 09:30 open · cash $10,305.73 · no holdings · equity $10,305.73 vs prior close $10,305.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,305.73 | ▲ close $10,305.73 vs 09:30 $10,305.73 (session +0.00) | 16:00 close · cash $10,305.73 · no lots left · equity $10,305.73. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,305.73 | ▲ 09:30 equity $10,305.73 vs yday $10,305.73 (+0.00) | 09:30 open · cash $10,305.73 · no holdings · equity $10,305.73 vs prior close $10,305.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,305.73 | ▲ close $10,305.73 vs 09:30 $10,305.73 (session +0.00) | 16:00 close · cash $10,305.73 · no lots left · equity $10,305.73. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,305.73 | ▲ 09:30 equity $10,305.73 vs yday $10,305.73 (+0.00) | 09:30 open · cash $10,305.73 · no holdings · equity $10,305.73 vs prior close $10,305.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 45 | $112.83 | $2.31 | — | $15,380.99 | — | short morning packet news🔴; gate news_box=bad; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $5152.87 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,380.99 | ▼ close $10,131.74 vs 09:30 $10,305.73 (session -171.68) | 16:00 close · cash $15,380.99 · equity $10,131.74 vs 09:30 $10,305.73 (-173.99; session marks -171.68) · 1 name(s) marked open→close (per-name table). QRVO×45 09:30 $112.83 → close $116.65 -171.68 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,380.99 | ▲ 09:30 equity $10,246.04 vs yday $10,131.74 (+114.30) | 09:30 open · cash $15,380.99 (unchanged overnight, no fees) · equity $10,246.04 vs prior close $10,131.74 (+114.30) · 1 name(s) re-marked at the open (per-name table). QRVO×45 yday $116.65 → 09:30 $114.11 +114.30 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,380.99 | ▲ close $10,521.89 vs 09:30 $10,246.04 (session +275.85) | 16:00 close · cash $15,380.99 · equity $10,521.89 vs 09:30 $10,246.04 (+275.85; session marks +275.85) · 1 name(s) marked open→close (per-name table). QRVO×45 09:30 $114.11 → close $107.98 +275.85 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,380.99 | ▼ 09:30 equity $10,502.99 vs yday $10,521.89 (-18.90) | 09:30 open · cash $15,380.99 (unchanged overnight, no fees) · equity $10,502.99 vs prior close $10,521.89 (-18.90) · 1 name(s) re-marked at the open (per-name table). QRVO×45 yday $107.98 → 09:30 $108.40 -18.90 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,380.99 | ▼ close $10,068.29 vs 09:30 $10,502.99 (session -434.70) | 16:00 close · cash $15,380.99 · equity $10,068.29 vs 09:30 $10,502.99 (-434.70; session marks -434.70) · 1 name(s) marked open→close (per-name table). QRVO×45 09:30 $108.40 → close $118.06 -434.70 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,380.99 | ▼ 09:30 equity $10,062.89 vs yday $10,068.29 (-5.40) | 09:30 open · cash $15,380.99 (unchanged overnight, no fees) · equity $10,062.89 vs prior close $10,068.29 (-5.40) · 1 name(s) re-marked at the open (per-name table). QRVO×45 yday $118.06 → 09:30 $118.18 -5.40 | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 45 | $118.18 | $2.12 | $-244.96 | $10,060.77 | ▼ -244.96 after sell → book $10,060.77; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,060.77 | ▲ close $10,060.77 vs 09:30 $10,062.89 (session +0.00) | 16:00 close · cash $10,060.77 · no lots left · equity $10,060.77. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,060.77 | ▲ 09:30 equity $10,060.77 vs yday $10,060.77 (-0.00) | 09:30 open · cash $10,060.77 · no holdings · equity $10,060.77 vs prior close $10,060.77 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,060.77 | ▲ close $10,060.77 vs 09:30 $10,060.77 (session +0.00) | 16:00 close · cash $10,060.77 · no lots left · equity $10,060.77. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,060.77 | ▲ 09:30 equity $10,060.77 vs yday $10,060.77 (-0.00) | 09:30 open · cash $10,060.77 · no holdings · equity $10,060.77 vs prior close $10,060.77 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,060.77 | ▲ close $10,060.77 vs 09:30 $10,060.77 (session +0.00) | 16:00 close · cash $10,060.77 · no lots left · equity $10,060.77. | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,060.77 | ▲ 09:30 equity $10,060.77 vs yday $10,060.77 (-0.00) | 09:30 open · cash $10,060.77 · no holdings · equity $10,060.77 vs prior close $10,060.77 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,060.77 | ▲ close $10,060.77 vs 09:30 $10,060.77 (session +0.00) | 16:00 close · cash $10,060.77 · no lots left · equity $10,060.77. | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,060.77 | ▲ 09:30 equity $10,060.77 vs yday $10,060.77 (-0.00) | 09:30 open · cash $10,060.77 · no holdings · equity $10,060.77 vs prior close $10,060.77 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,060.77 | ▲ close $10,060.77 vs 09:30 $10,060.77 (session +0.00) | 16:00 close · cash $10,060.77 · no lots left · equity $10,060.77. | — |

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
| 2026-08-24 | `BMO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `INTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `NEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `NEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `FIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `FIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-14 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
