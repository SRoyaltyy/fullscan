# Factor mine action — `union_news_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_g, no 🚨

Cash book **-14.92%** ($8,508) · signal-only (no cash/fees) was +8.62%. Starts YES **0/30**. Fills 199 · skips 78 · realized $-274.79.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the news camera (does the morning packet like the headline?) is green.
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
- **Gate** `news=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,725.22.

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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $5,285.64 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $4,050.55 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $2,801.68 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $1,560.49 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,560.49 | ▲ close $10,110.67 vs 09:30 $10,000.00 (session +127.16) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,560.49 | ▲ 09:30 equity $10,211.68 vs yday $10,110.67 (+101.01) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $2,662.11 | ▲ +20.13 after sell → book $10,209.66; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $3,855.04 | ▲ +15.71 after sell → book $10,207.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $5,127.00 | ▲ +69.94 after sell → book $10,205.59; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $6,457.20 | ▲ +76.56 after sell → book $10,201.79; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,687.91 | ▼ -4.38 after sell → book $10,199.59; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $8,896.34 | ▼ -40.44 after sell → book $10,197.30; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $10,195.00 | ▲ +57.47 after sell → book $10,195.00; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 44 | $46.18 | $2.12 | — | $8,160.96 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+6.7; leftover $2039.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 14 | $142.77 | $2.03 | — | $6,160.14 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+5.8; leftover $2039.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 10 | $202.70 | $2.02 | — | $4,131.12 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ret5=+8.3; leftover $2039.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 21 | $92.99 | $2.05 | — | $2,176.28 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.8; leftover $2039.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 41 | $49.00 | $2.11 | — | $165.17 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ⚪; ret5=+12.2; leftover $2039.00 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $165.17 | ▲ close $10,281.82 vs 09:30 $10,211.68 (session +97.16) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $165.17 | ▼ 09:30 equity $10,227.70 vs yday $10,281.82 (-54.12) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 44 | $48.00 | $2.15 | $+75.81 | $2,275.02 | ▲ +75.81 after sell → book $10,225.55; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 14 | $148.04 | $2.06 | $+69.69 | $4,345.52 | ▲ +69.69 after sell → book $10,223.49; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 10 | $208.93 | $2.05 | $+58.23 | $6,432.77 | ▲ +58.23 after sell → book $10,221.44; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 21 | $92.38 | $2.08 | $-16.94 | $8,370.67 | ▼ -16.94 after sell → book $10,219.36; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 41 | $45.09 | $2.14 | $-164.56 | $10,217.23 | ▼ -164.56 after sell → book $10,217.23; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,217.23 | ▲ close $10,217.23 vs 09:30 $10,227.70 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.23 | ▲ 09:30 equity $10,217.23 vs yday $10,217.23 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,217.23 | ▲ close $10,217.23 vs 09:30 $10,217.23 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,217.23 | ▲ 09:30 equity $10,217.23 vs yday $10,217.23 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,941.05 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1277.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,737.92 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1277.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1806 | $0.71 | $18.19 | — | $6,442.89 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1277.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 193 | $6.61 | $2.57 | — | $5,165.56 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1277.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,899.33 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1277.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 48 | $26.57 | $2.13 | — | $2,621.84 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1277.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $1,386.45 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1277.15 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $131.10 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ret5=+8.7; leftover $1277.15 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.10 | ▼ close $9,998.84 vs 09:30 $10,217.23 (session -185.11) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.10 | ▲ 09:30 equity $10,250.47 vs yday $9,998.84 (+251.63) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 14 | $95.72 | $2.05 | $+61.86 | $1,469.13 | ▲ +61.86 after sell → book $10,248.42; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $2,531.97 | ▼ -140.29 after sell → book $10,246.39; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1806 | $0.67 | $17.90 | $-95.68 | $3,731.32 | ▼ -95.68 after sell → book $10,228.49; vs 09:30 mark -17.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 193 | $6.95 | $2.61 | $+61.40 | $5,070.06 | ▲ +61.40 after sell → book $10,225.88; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $6,462.94 | ▲ +126.66 after sell → book $10,223.62; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 48 | $26.25 | $2.15 | $-19.65 | $7,720.79 | ▼ -19.65 after sell → book $10,221.47; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $8,965.26 | ▼ -10.89 after sell → book $10,219.38; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $7,768.94 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1280.75 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 518 | $2.47 | $6.68 | — | $6,482.79 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1280.75 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,213.79 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1280.75 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $3,965.28 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1280.75 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 109 | $11.70 | $2.32 | — | $2,687.66 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1280.75 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 115 | $11.10 | $2.33 | — | $1,409.40 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+19.1; leftover $1280.75 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 395 | $3.24 | $5.10 | — | $124.50 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1280.75 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.50 | ▲ close $10,221.30 vs 09:30 $10,250.47 (session +24.39) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.50 | ▼ 09:30 equity $10,162.41 vs yday $10,221.30 (-58.89) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,327.56 | ▲ +6.74 after sell → book $10,160.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 518 | $2.40 | $6.78 | $-49.72 | $2,563.98 | ▼ -49.72 after sell → book $10,153.59; vs 09:30 mark -6.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $3,892.94 | ▲ +59.95 after sell → book $10,151.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.04 | $2.02 | $+55.55 | $5,197.00 | ▲ +55.55 after sell → book $10,149.53; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 109 | $11.17 | $2.35 | $-62.43 | $6,412.19 | ▼ -62.43 after sell → book $10,147.19; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 115 | $11.48 | $2.36 | $+39.58 | $7,730.02 | ▲ +39.58 after sell → book $10,144.82; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 395 | $2.99 | $5.17 | $-109.02 | $8,905.90 | ▼ -109.02 after sell → book $10,139.65; vs 09:30 mark -5.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,905.90 | ▼ close $10,104.48 vs 09:30 $10,162.41 (session -35.17) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,905.90 | ▲ 09:30 equity $10,122.43 vs yday $10,104.48 (+17.95) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $10,120.36 | ▼ -20.93 after sell → book $10,120.36; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 153 | $9.42 | $2.45 | — | $8,676.65 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1445.77 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 41 | $35.05 | $2.11 | — | $7,237.49 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1445.77 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 59 | $24.11 | $2.17 | — | $5,812.83 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=+891.7; leftover $1445.77 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 50 | $28.86 | $2.14 | — | $4,367.69 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.7; leftover $1445.77 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 165 | $8.72 | $2.48 | — | $2,926.41 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+13.0; leftover $1445.77 | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 12 | $118.52 | $2.03 | — | $1,502.14 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1445.77 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 18 | $77.13 | $2.04 | — | $111.76 | — | union ∩ news_g, no 🚨; gate news=good; list mover_buy; ⚪; ret5=+13.8; leftover $1445.77 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.76 | ▲ close $10,572.18 vs 09:30 $10,122.43 (session +467.24) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.76 | ▼ 09:30 equity $10,391.78 vs yday $10,572.18 (-180.40) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 153 | $10.07 | $2.49 | $+94.51 | $1,649.98 | ▲ +94.51 after sell → book $10,389.29; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 41 | $35.70 | $2.13 | $+22.40 | $3,111.55 | ▲ +22.40 after sell → book $10,387.16; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 59 | $26.61 | $2.19 | $+143.14 | $4,679.35 | ▲ +143.14 after sell → book $10,384.97; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 50 | $27.56 | $2.16 | $-69.30 | $6,055.18 | ▼ -69.30 after sell → book $10,382.80; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 165 | $8.86 | $2.52 | $+18.09 | $7,514.56 | ▲ +18.09 after sell → book $10,380.28; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 12 | $119.80 | $2.05 | $+11.29 | $8,950.11 | ▲ +11.29 after sell → book $10,378.23; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 18 | $79.34 | $2.07 | $+35.67 | $10,376.17 | ▲ +35.67 after sell → book $10,376.17; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 155 | $11.12 | $2.46 | — | $8,650.11 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1729.36 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 208 | $8.29 | $2.68 | — | $6,923.11 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $1729.36 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 99 | $17.41 | $2.29 | — | $5,197.23 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-9.2; leftover $1729.36 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 154 | $11.22 | $2.45 | — | $3,466.90 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.8; leftover $1729.36 | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 6 | $267.02 | $2.01 | — | $1,862.77 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+16.7; leftover $1729.36 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 14 | $118.50 | $2.03 | — | $201.74 | — | union ∩ news_g, no 🚨; gate news=good; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $1729.36 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $201.74 | ▲ close $10,630.63 vs 09:30 $10,391.78 (session +268.38) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $201.74 | ▲ 09:30 equity $10,659.94 vs yday $10,630.63 (+29.31) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 6 | $267.23 | $2.03 | $-2.78 | $1,803.09 | ▼ -2.78 after sell → book $10,657.91; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 14 | $41.44 | $2.03 | — | $1,220.90 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; ret5=+3.1; leftover $601.03 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 8 | $70.30 | $2.01 | — | $656.48 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=-11.2; leftover $601.03 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 10 | $60.00 | $2.02 | — | $54.46 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+6.2; leftover $601.03 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.46 | ▲ close $10,688.19 vs 09:30 $10,659.94 (session +36.35) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.46 | ▼ 09:30 equity $10,584.87 vs yday $10,688.19 (-103.32) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 155 | $11.27 | $2.49 | $+18.30 | $1,798.82 | ▲ +18.30 after sell → book $10,582.38; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWRD` | 99 | $17.70 | $2.32 | $+24.11 | $3,548.80 | ▲ +24.11 after sell → book $10,580.06; vs 09:30 mark -2.32 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 154 | $11.00 | $2.49 | $-38.82 | $5,240.31 | ▼ -38.82 after sell → book $10,577.57; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 14 | $115.66 | $2.06 | $-43.85 | $6,857.50 | ▼ -43.85 after sell → book $10,575.52; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 8 | $65.29 | $2.03 | $-44.13 | $7,377.78 | ▼ -44.13 after sell → book $10,573.48; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 10 | $58.75 | $2.04 | $-16.56 | $7,963.24 | ▼ -16.56 after sell → book $10,571.44; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 40 | $32.90 | $2.11 | — | $6,645.13 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1327.21 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 154 | $8.61 | $2.45 | — | $5,316.74 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; ret5=-0.7; leftover $1327.21 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $4,038.88 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1327.21 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 68 | $19.25 | $2.19 | — | $2,727.69 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+14.1; leftover $1327.21 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 70 | $18.75 | $2.20 | — | $1,412.99 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-5.0; leftover $1327.21 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 45 | $28.91 | $2.12 | — | $109.91 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=+9.2; leftover $1327.21 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.91 | ▼ close $10,277.67 vs 09:30 $10,584.87 (session -280.67) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.91 | ▼ 09:30 equity $10,248.05 vs yday $10,277.67 (-29.62) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 208 | $9.50 | $2.73 | $+246.26 | $2,083.18 | ▲ +246.26 after sell → book $10,245.32; vs 09:30 mark -2.73 | dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 14 | $42.00 | $2.05 | $+3.76 | $2,669.13 | ▲ +3.76 after sell → book $10,243.27; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 40 | $31.15 | $2.13 | $-74.24 | $3,913.00 | ▼ -74.24 after sell → book $10,241.14; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 154 | $8.52 | $2.49 | $-18.80 | $5,222.59 | ▼ -18.80 after sell → book $10,238.65; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $6,411.25 | ▼ -89.19 after sell → book $10,236.61; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 68 | $17.87 | $2.22 | $-98.25 | $7,624.20 | ▼ -98.25 after sell → book $10,234.40; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 70 | $19.25 | $2.22 | $+30.58 | $8,969.47 | ▲ +30.58 after sell → book $10,232.17; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 45 | $28.06 | $2.15 | $-42.52 | $10,230.03 | ▼ -42.52 after sell → book $10,230.03; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,230.03 | ▲ close $10,230.03 vs 09:30 $10,248.05 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,230.03 | ▲ 09:30 equity $10,230.03 vs yday $10,230.03 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,230.03 | ▲ close $10,230.03 vs 09:30 $10,230.03 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,230.03 | ▲ 09:30 equity $10,230.03 vs yday $10,230.03 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,230.03 | ▲ close $10,230.03 vs 09:30 $10,230.03 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,230.03 | ▲ 09:30 equity $10,230.03 vs yday $10,230.03 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $23.88 | $2.15 | — | $8,962.24 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1278.75 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 38 | $32.88 | $2.10 | — | $7,710.70 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+16.2; leftover $1278.75 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 168 | $7.59 | $2.49 | — | $6,433.08 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.5; leftover $1278.75 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $5,727.84 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1278.75 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 80 | $15.87 | $2.23 | — | $4,456.01 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1278.75 | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $3,398.79 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+3.3; leftover $1278.75 | — |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $354.49 | $2.00 | — | $2,333.32 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-12.3; leftover $1278.75 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 26 | $47.60 | $2.07 | — | $1,093.65 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=-6.2; leftover $1278.75 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,093.65 | ▲ close $10,394.86 vs 09:30 $10,230.03 (session +181.87) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,093.65 | ▼ 09:30 equity $10,348.37 vs yday $10,394.86 (-46.49) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 53 | $23.84 | $2.17 | $-6.44 | $2,355.00 | ▼ -6.44 after sell → book $10,346.20; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 38 | $32.48 | $2.12 | $-19.43 | $3,587.12 | ▼ -19.43 after sell → book $10,344.08; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 168 | $7.79 | $2.53 | $+28.57 | $4,893.31 | ▲ +28.57 after sell → book $10,341.55; vs 09:30 mark -2.53 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $5,583.33 | ▼ -15.23 after sell → book $10,339.54; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $6,660.41 | ▲ +19.86 after sell → book $10,337.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $321.67 | $2.02 | $-102.48 | $7,623.40 | ▼ -102.48 after sell → book $10,335.50; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 26 | $53.85 | $2.09 | $+158.34 | $9,021.41 | ▲ +158.34 after sell → book $10,333.41; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 6 | $263.36 | $2.01 | — | $7,439.24 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1804.28 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 930 | $1.94 | $12.00 | — | $5,623.04 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+18.3; leftover $1804.28 | — |
| 2026-09-04 09:30 ET | **BUY** | `MSTR` | 13 | $137.35 | $2.03 | — | $3,835.46 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+5.4; leftover $1804.28 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 7 | $236.82 | $2.01 | — | $2,175.71 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.1; leftover $1804.28 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 23 | $75.65 | $2.06 | — | $433.70 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1804.28 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $433.70 | ▲ close $10,478.28 vs 09:30 $10,348.37 (session +164.98) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $433.70 | ▲ 09:30 equity $10,576.12 vs yday $10,478.28 (+97.84) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 80 | $16.74 | $2.25 | $+65.12 | $1,770.65 | ▲ +65.12 after sell → book $10,573.87; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 6 | $253.72 | $2.03 | $-61.88 | $3,290.94 | ▼ -61.88 after sell → book $10,571.84; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 930 | $1.94 | $12.17 | $-24.16 | $5,082.97 | ▼ -24.16 after sell → book $10,559.67; vs 09:30 mark -12.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 7 | $267.76 | $2.04 | $+212.53 | $6,955.26 | ▲ +212.53 after sell → book $10,557.64; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,955.26 | ▼ close $10,494.35 vs 09:30 $10,576.12 (session -63.29) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,955.26 | ▲ 09:30 equity $10,560.72 vs yday $10,494.35 (+66.37) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MSTR` | 13 | $141.82 | $2.05 | $+54.03 | $8,796.86 | ▲ +54.03 after sell → book $10,558.66; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 23 | $76.60 | $2.08 | $+17.71 | $10,556.58 | ▲ +17.71 after sell → book $10,556.58; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,556.58 | ▲ close $10,556.58 vs 09:30 $10,560.72 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,556.58 | ▲ 09:30 equity $10,556.58 vs yday $10,556.58 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,556.58 | ▲ close $10,556.58 vs 09:30 $10,556.58 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,556.58 | ▲ 09:30 equity $10,556.58 vs yday $10,556.58 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 10 | $164.43 | $2.02 | — | $8,910.26 | — | union ∩ news_g, no 🚨; gate news=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1759.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 862 | $2.04 | $11.12 | — | $7,140.66 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1759.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 829 | $2.12 | $10.69 | — | $5,372.49 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1759.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 117 | $15.01 | $2.34 | — | $3,613.98 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+4.9; leftover $1759.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 7 | $242.17 | $2.01 | — | $1,916.78 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-11.1; leftover $1759.43 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 12 | $135.71 | $2.03 | — | $286.23 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; ret5=-9.2; leftover $1759.43 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $286.23 | ▼ close $10,353.19 vs 09:30 $10,556.58 (session -173.18) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $286.23 | ▼ 09:30 equity $10,279.66 vs yday $10,353.19 (-73.53) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 10 | $141.42 | $2.04 | $-234.16 | $1,698.39 | ▼ -234.16 after sell → book $10,277.62; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 862 | $2.01 | $11.28 | $-48.26 | $3,419.73 | ▼ -48.26 after sell → book $10,266.34; vs 09:30 mark -11.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 829 | $2.05 | $10.85 | $-79.57 | $5,108.34 | ▼ -79.57 after sell → book $10,255.50; vs 09:30 mark -10.84 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 7 | $261.51 | $2.04 | $+131.33 | $6,936.87 | ▲ +131.33 after sell → book $10,253.46; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 12 | $131.40 | $2.05 | $-55.79 | $8,511.62 | ▼ -55.79 after sell → book $10,251.41; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,511.62 | ▲ close $10,285.34 vs 09:30 $10,279.66 (session +33.93) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,511.62 | ▲ 09:30 equity $10,292.36 vs yday $10,285.34 (+7.02) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,511.62 | ▲ close $10,318.10 vs 09:30 $10,292.36 (session +25.74) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,511.62 | ▲ 09:30 equity $10,328.63 vs yday $10,318.10 (+10.53) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 81 | $26.27 | $2.23 | — | $6,381.52 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=+10.0; leftover $2127.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 306 | $6.95 | $3.95 | — | $4,250.87 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-5.8; leftover $2127.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 53 | $39.99 | $2.15 | — | $2,129.25 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+9.3; leftover $2127.91 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 11 | $189.17 | $2.02 | — | $46.36 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+7.9; leftover $2127.91 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.36 | ▼ close $10,236.48 vs 09:30 $10,328.63 (session -81.80) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.36 | ▲ 09:30 equity $10,353.12 vs yday $10,236.48 (+116.64) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 81 | $26.51 | $2.26 | $+14.94 | $2,191.41 | ▲ +14.94 after sell → book $10,350.86; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 306 | $7.27 | $4.02 | $+89.96 | $4,412.01 | ▲ +89.96 after sell → book $10,346.84; vs 09:30 mark -4.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 53 | $37.57 | $2.17 | $-132.58 | $6,401.04 | ▼ -132.58 after sell → book $10,344.66; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 11 | $190.35 | $2.05 | $+8.91 | $8,492.85 | ▲ +8.91 after sell → book $10,342.62; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 8 | $170.85 | $2.01 | — | $7,124.03 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1415.47 | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $6,187.16 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; ret5=-7.0; leftover $1415.47 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 79 | $17.72 | $2.23 | — | $4,785.05 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer; 🔵; ret5=-8.3; leftover $1415.47 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 799 | $1.77 | $10.31 | — | $3,360.51 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-10.2; leftover $1415.47 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $2,165.51 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-11.6; leftover $1415.47 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 63 | $22.12 | $2.18 | — | $769.77 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+10.5; leftover $1415.47 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $769.77 | ▲ close $10,354.41 vs 09:30 $10,353.12 (session +32.52) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $769.77 | ▲ 09:30 equity $10,395.06 vs yday $10,354.41 (+40.65) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 117 | $15.87 | $2.38 | $+95.90 | $2,624.18 | ▲ +95.90 after sell → book $10,392.68; vs 09:30 mark -2.38 | dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 8 | $182.33 | $2.04 | $+87.79 | $4,080.79 | ▲ +87.79 after sell → book $10,390.65; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $4,994.44 | ▼ -23.23 after sell → book $10,388.64; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 79 | $17.13 | $2.25 | $-51.09 | $6,345.46 | ▼ -51.09 after sell → book $10,386.39; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 799 | $1.77 | $10.45 | $-20.76 | $7,749.23 | ▼ -20.76 after sell → book $10,375.93; vs 09:30 mark -10.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $8,931.21 | ▼ -13.03 after sell → book $10,373.91; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 158 | $14.07 | $2.46 | — | $6,705.69 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2232.80 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 150 | $14.79 | $2.44 | — | $4,484.75 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2232.80 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 296 | $7.54 | $3.82 | — | $2,250.57 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-20.9; leftover $2232.80 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 106 | $20.91 | $2.31 | — | $31.80 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2232.80 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.80 | ▼ close $10,199.44 vs 09:30 $10,395.06 (session -163.44) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.80 | ▲ 09:30 equity $10,323.60 vs yday $10,199.44 (+124.16) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 63 | $22.78 | $2.20 | $+37.20 | $1,464.74 | ▲ +37.20 after sell → book $10,321.40; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 158 | $13.90 | $2.51 | $-31.83 | $3,658.43 | ▼ -31.83 after sell → book $10,318.89; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 150 | $14.58 | $2.48 | $-36.42 | $5,842.95 | ▼ -36.42 after sell → book $10,316.41; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 296 | $7.36 | $3.88 | $-59.50 | $8,017.62 | ▼ -59.50 after sell → book $10,312.52; vs 09:30 mark -3.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 106 | $21.65 | $2.34 | $+73.79 | $10,310.18 | ▲ +73.79 after sell → book $10,310.18; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 66 | $25.95 | $2.19 | — | $8,595.29 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1718.36 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 799 | $2.15 | $10.31 | — | $6,867.13 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1718.36 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 123 | $13.94 | $2.36 | — | $5,150.15 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1718.36 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 286 | $6.00 | $3.69 | — | $3,430.47 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; ret5=-24.1; leftover $1718.36 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 9 | $190.30 | $2.02 | — | $1,715.75 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+10.6; leftover $1718.36 | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 7 | $230.25 | $2.01 | — | $101.99 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+12.5; leftover $1718.36 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.99 | ▼ close $10,005.59 vs 09:30 $10,323.60 (session -282.02) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.99 | ▼ 09:30 equity $9,984.28 vs yday $10,005.59 (-21.31) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 123 | $13.13 | $2.39 | $-104.38 | $1,714.58 | ▼ -104.38 after sell → book $9,981.88; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 286 | $5.99 | $3.75 | $-10.30 | $3,423.97 | ▼ -10.30 after sell → book $9,978.13; vs 09:30 mark -3.75 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 565 | $1.01 | $7.29 | — | $2,846.04 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; ret5=+14.3; leftover $570.66 | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 3 | $168.50 | $2.00 | — | $2,338.54 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+17.9; leftover $570.66 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 132 | $4.30 | $2.39 | — | $1,768.55 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+16.9; leftover $570.66 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,768.55 | ▲ close $9,972.09 vs 09:30 $9,984.28 (session +5.63) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,768.55 | ▲ 09:30 equity $10,283.31 vs yday $9,972.09 (+311.22) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 66 | $26.58 | $2.21 | $+37.18 | $3,520.62 | ▲ +37.18 after sell → book $10,281.09; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 799 | $2.09 | $10.45 | $-68.70 | $5,180.07 | ▼ -68.70 after sell → book $10,270.64; vs 09:30 mark -10.45 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 9 | $174.50 | $2.04 | $-146.26 | $6,748.53 | ▼ -146.26 after sell → book $10,268.60; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 7 | $266.50 | $2.04 | $+249.70 | $8,612.00 | ▲ +249.70 after sell → book $10,266.56; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 565 | $0.95 | $7.17 | $-48.35 | $9,141.58 | ▼ -48.35 after sell → book $10,259.40; vs 09:30 mark -7.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MRNA` | 3 | $183.41 | $2.02 | $+40.70 | $9,689.78 | ▲ +40.70 after sell → book $10,257.38; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 203 | $7.95 | $2.62 | — | $8,073.31 | — | union ∩ news_g, no 🚨; gate news=good; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1614.96 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 102 | $15.72 | $2.30 | — | $6,467.57 | — | union ∩ news_g, no 🚨; gate news=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1614.96 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 1323 | $1.22 | $17.07 | — | $4,836.45 | — | union ∩ news_g, no 🚨; gate news=good; list yday_mover; 🔵; ret5=-33.0; leftover $1614.96 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 1242 | $1.30 | $16.02 | — | $3,205.83 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+15.3; leftover $1614.96 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 40 | $40.00 | $2.11 | — | $1,603.72 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ret5=+6.7; leftover $1614.96 | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 8 | $196.78 | $2.01 | — | $27.46 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $1614.96 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.46 | ▼ close $9,870.19 vs 09:30 $10,283.31 (session -345.06) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.46 | ▼ 09:30 equity $9,770.33 vs yday $9,870.19 (-99.86) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 132 | $4.12 | $2.42 | $-28.56 | $568.88 | ▼ -28.56 after sell → book $9,767.91; vs 09:30 mark -2.42 | dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 203 | $7.38 | $2.66 | $-120.99 | $2,064.36 | ▼ -120.99 after sell → book $9,765.25; vs 09:30 mark -2.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 102 | $14.38 | $2.32 | $-141.30 | $3,528.79 | ▼ -141.30 after sell → book $9,762.92; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 1323 | $1.17 | $17.30 | $-100.52 | $5,059.41 | ▼ -100.52 after sell → book $9,745.63; vs 09:30 mark -17.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 1242 | $1.27 | $16.24 | $-69.52 | $6,620.51 | ▼ -69.52 after sell → book $9,729.39; vs 09:30 mark -16.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 40 | $39.27 | $2.13 | $-33.44 | $8,189.17 | ▼ -33.44 after sell → book $9,727.25; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 8 | $192.26 | $2.04 | $-40.21 | $9,725.22 | ▼ -40.21 after sell → book $9,725.22; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,725.22 | ▲ close $9,725.22 vs 09:30 $9,770.33 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,565.75 | ▲ 09:30 equity $8,565.75 vs yday $8,565.75 (+0.00) | 09:30 open · cash $8,565.75 · no holdings · equity $8,565.75 vs prior close $8,565.75 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 443 | $3.86 | $5.71 | — | $6,850.06 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1713.15 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 105 | $16.21 | $2.31 | — | $5,145.70 | — | union ∩ news_g, no 🚨; gate news=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1713.15 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 6 | $272.16 | $2.01 | — | $3,510.73 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1713.15 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 23 | $74.15 | $2.06 | — | $1,803.22 | — | union ∩ news_g, no 🚨; gate news=good; list ohlc_hot; ret5=+8.5; leftover $1713.15 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $914.23 | — | union ∩ news_g, no 🚨; gate news=good; list earn_react; 🔵; ret5=+0.3; leftover $1713.15 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $914.23 | ▼ close $8,508.19 vs 09:30 $8,565.75 (session -43.49) | 16:00 close · cash $914.23 · equity $8,508.19 vs 09:30 $8,565.75 (-57.56; session marks -43.49) · 5 name(s) marked open→close (per-name table). ZSQR×443 09:30 $3.86 → close $3.78 -35.44; SECZ×105 09:30 $16.21 → close $15.96 -26.25; ILMN×6 09:30 $272.16 → close $270.00 -12.96; RKLB×23 09:30 $74.15 → close $73.95 -4.60; COST×1 09:30 $887.00 → close $922.76 +35.76 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |
