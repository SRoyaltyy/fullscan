# Factor mine action — `short_news_pack_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · short morning packet news🔴

Cash book **+3.06%** ($10,306) · signal-only (no cash/fees) was -6.15%. Starts YES **9/30**. Fills 26 · skips 26 · realized $+60.75.

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

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **SHORT** | `AEM` | 8 | $204.45 | $2.08 | — | $11,633.52 | — | short morning packet news🔴; gate news_box=bad; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1666.67 | — |
| 2026-08-20 09:30 ET | **SHORT** | `TEAM` | 9 | $173.90 | $2.08 | — | $13,196.54 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; 🔵; ret5=+12.2; leftover $1666.67 | — |
| 2026-08-20 09:30 ET | **SHORT** | `WMT` | 15 | $106.38 | $2.10 | — | $14,790.13 | — | short morning packet news🔴; gate news_box=bad; list earn_react; 🔵; ret5=-1.7; leftover $1666.67 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,790.13 | ▼ close $9,962.02 vs 09:30 $10,000.00 (session -31.71) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,790.13 | ▼ 09:30 equity $9,936.40 vs yday $9,962.02 (-25.62) | — | — |
| 2026-08-21 09:30 ET | **SHORT** | `AUGO` | 27 | $89.10 | $2.17 | — | $17,193.67 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $2484.10 | — |
| 2026-08-21 09:30 ET | **SHORT** | `SSRM` | 64 | $38.40 | $2.28 | — | $19,648.99 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; 🔵; ret5=+15.8; leftover $2484.10 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,648.99 | ▲ close $10,045.42 vs 09:30 $9,936.40 (session +113.46) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,648.99 | ▼ 09:30 equity $9,982.27 vs yday $10,045.42 (-63.15) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,648.99 | ▼ close $9,936.52 vs 09:30 $9,982.27 (session -45.75) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,648.99 | ▲ 09:30 equity $10,101.47 vs yday $9,936.52 (+164.95) | — | — |
| 2026-08-25 09:30 ET | **COVER** | `AEM` | 8 | $212.00 | $2.01 | $-64.50 | $17,950.97 | ▼ -64.50 after sell → book $10,099.45; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `TEAM` | 9 | $170.64 | $2.02 | $+25.24 | $16,413.20 | ▲ +25.24 after sell → book $10,097.44; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `WMT` | 15 | $105.58 | $2.04 | $+7.86 | $14,827.46 | ▲ +7.86 after sell → book $10,095.40; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `ARE` | 30 | $54.51 | $2.15 | — | $16,460.61 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; ret5=+15.1; leftover $1682.57 | — |
| 2026-08-25 09:30 ET | **SHORT** | `BMO` | 9 | $175.01 | $2.08 | — | $18,033.62 | — | short morning packet news🔴; gate news_box=bad; list earn_react; ret5=-7.0; leftover $1682.57 | — |
| 2026-08-25 09:30 ET | **SHORT** | `INTU` | 4 | $364.35 | $2.06 | — | $19,488.96 | — | short morning packet news🔴; gate news_box=bad; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $1682.57 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,488.96 | ▼ close $9,958.85 vs 09:30 $10,101.47 (session -130.26) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,488.96 | ▲ 09:30 equity $10,212.28 vs yday $9,958.85 (+253.43) | — | — |
| 2026-08-26 09:30 ET | **COVER** | `AUGO` | 27 | $88.24 | $2.07 | $+18.98 | $17,104.41 | ▲ +18.98 after sell → book $10,210.21; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **COVER** | `SSRM` | 64 | $38.41 | $2.18 | $-5.10 | $14,643.99 | ▼ -5.10 after sell → book $10,208.03; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SHORT** | `BE` | 7 | $213.94 | $2.07 | — | $16,139.49 | — | short morning packet news🔴; gate news_box=bad; list probable,yday_gainer,yday_mover; 🔵; ret5=+4.0; leftover $1701.34 | — |
| 2026-08-26 09:30 ET | **SHORT** | `NEM` | 12 | $132.64 | $2.09 | — | $17,729.08 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; 🔵; ret5=+16.5; leftover $1701.34 | — |
| 2026-08-26 09:30 ET | **SHORT** | `CRM` | 8 | $199.94 | $2.08 | — | $19,326.52 | — | short morning packet news🔴; gate news_box=bad; list overnight,overnight_mega; ret5=+2.1; leftover $1701.34 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,326.52 | ▼ close $10,046.17 vs 09:30 $10,212.28 (session -155.61) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,326.52 | ▼ 09:30 equity $9,780.87 vs yday $10,046.17 (-265.30) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,326.52 | ▼ close $9,688.26 vs 09:30 $9,780.87 (session -92.61) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,326.52 | ▲ 09:30 equity $9,703.80 vs yday $9,688.26 (+15.54) | — | — |
| 2026-08-28 09:30 ET | **COVER** | `ARE` | 30 | $52.49 | $2.08 | $+56.37 | $17,749.74 | ▲ +56.37 after sell → book $9,701.72; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMO` | 9 | $172.76 | $2.02 | $+16.15 | $16,192.88 | ▲ +16.15 after sell → book $9,699.71; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `INTU` | 4 | $347.82 | $2.00 | $+62.05 | $14,799.60 | ▲ +62.05 after sell → book $9,697.70; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIG` | 160 | $30.18 | $2.67 | — | $19,625.73 | — | short morning packet news🔴; gate news_box=bad; list ohlc_hot; ret5=+12.1; leftover $4848.85 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,625.73 | ▲ close $9,955.38 vs 09:30 $9,703.80 (session +260.35) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,625.73 | ▲ 09:30 equity $10,183.05 vs yday $9,955.38 (+227.67) | — | — |
| 2026-08-31 09:30 ET | **COVER** | `BE` | 7 | $208.88 | $2.01 | $+31.33 | $18,161.56 | ▲ +31.33 after sell → book $10,181.04; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `NEM` | 12 | $127.45 | $2.03 | $+58.16 | $16,630.13 | ▲ +58.16 after sell → book $10,179.01; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **COVER** | `CRM` | 8 | $254.39 | $2.01 | $-439.69 | $14,593.00 | ▼ -439.69 after sell → book $10,177.00; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,593.00 | ▲ close $10,194.60 vs 09:30 $10,183.05 (session +17.60) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,593.00 | ▲ 09:30 equity $10,263.40 vs yday $10,194.60 (+68.80) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,593.00 | ▼ close $10,241.00 vs 09:30 $10,263.40 (session -22.40) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,593.00 | ▲ 09:30 equity $10,308.20 vs yday $10,241.00 (+67.20) | — | — |
| 2026-09-02 09:30 ET | **COVER** | `FIG` | 160 | $26.78 | $2.47 | $+538.86 | $10,305.73 | ▲ +538.86 after sell → book $10,305.73; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,305.73 | ▲ close $10,305.73 vs 09:30 $10,308.20 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,305.73 | ▲ 09:30 equity $10,305.73 vs yday $10,305.73 (+0.00) | — | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,305.73 | ▲ close $10,305.73 vs 09:30 $10,305.73 (session +0.00) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,305.73 | ▲ 09:30 equity $10,305.73 vs yday $10,305.73 (+0.00) | — | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,305.73 | ▲ close $10,305.73 vs 09:30 $10,305.73 (session +0.00) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,305.73 | ▲ 09:30 equity $10,305.73 vs yday $10,305.73 (+0.00) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,305.73 | ▲ close $10,305.73 vs 09:30 $10,305.73 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,305.73 | ▲ 09:30 equity $10,305.73 vs yday $10,305.73 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,305.73 | ▲ close $10,305.73 vs 09:30 $10,305.73 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,305.73 | ▲ 09:30 equity $10,305.73 vs yday $10,305.73 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,305.73 | ▲ close $10,305.73 vs 09:30 $10,305.73 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,305.73 | ▲ 09:30 equity $10,305.73 vs yday $10,305.73 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **SHORT** | `QRVO` | 45 | $112.83 | $2.31 | — | $15,380.99 | — | short morning packet news🔴; gate news_box=bad; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $5152.87 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,380.99 | ▼ close $10,131.74 vs 09:30 $10,305.73 (session -171.68) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,380.99 | ▲ 09:30 equity $10,246.04 vs yday $10,131.74 (+114.30) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,380.99 | ▲ close $10,521.89 vs 09:30 $10,246.04 (session +275.85) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,380.99 | ▼ 09:30 equity $10,502.99 vs yday $10,521.89 (-18.90) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,380.99 | ▼ close $10,068.29 vs 09:30 $10,502.99 (session -434.70) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,380.99 | ▼ 09:30 equity $10,062.89 vs yday $10,068.29 (-5.40) | — | — |
| 2026-09-16 09:30 ET | **COVER** | `QRVO` | 45 | $118.18 | $2.12 | $-244.96 | $10,060.77 | ▼ -244.96 after sell → book $10,060.77; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,060.77 | ▲ close $10,060.77 vs 09:30 $10,062.89 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,060.77 | ▲ 09:30 equity $10,060.77 vs yday $10,060.77 (-0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,060.77 | ▲ close $10,060.77 vs 09:30 $10,060.77 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,060.77 | ▲ 09:30 equity $10,060.77 vs yday $10,060.77 (-0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,060.77 | ▲ close $10,060.77 vs 09:30 $10,060.77 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,060.77 | ▲ 09:30 equity $10,060.77 vs yday $10,060.77 (-0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,060.77 | ▲ close $10,060.77 vs 09:30 $10,060.77 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,060.77 | ▲ 09:30 equity $10,060.77 vs yday $10,060.77 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,060.77 | ▲ close $10,060.77 vs 09:30 $10,060.77 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,060.77 | ▲ 09:30 equity $10,060.77 vs yday $10,060.77 (-0.00) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,060.77 | ▲ close $10,060.77 vs 09:30 $10,060.77 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,060.77 | ▲ 09:30 equity $10,060.77 vs yday $10,060.77 (-0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,060.77 | ▲ close $10,060.77 vs 09:30 $10,060.77 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,305.50 | ▲ 09:30 equity $10,305.50 vs yday $10,305.50 (+0.00) | 09:30 open · cash $10,305.50 · no holdings · equity $10,305.50 vs prior close $10,305.50 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,305.50 | ▲ close $10,305.50 vs 09:30 $10,305.50 (session +0.00) | 16:00 close · cash $10,305.50 · no lots left · equity $10,305.50. | — |

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
