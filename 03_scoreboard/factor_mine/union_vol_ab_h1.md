# Factor mine action — `union_vol_ab_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-5.01%** ($9,499) · signal-only (no cash/fees) was +53.27%. Starts YES **26/30**. Fills 264 · skips 65 · realized $+1139.59.

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
- Must-have: the A/B camera (does our A/B score like this name?) is green.
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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ab=good,vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,139.55.

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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.91 | ▲ close $10,208.28 vs 09:30 $10,000.00 (session +232.95) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.91 | ▲ 09:30 equity $10,475.50 vs yday $10,208.28 (+267.22) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 60 | $21.90 | $2.19 | $+76.64 | $1,498.71 | ▲ +76.64 after sell → book $10,473.30; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,741.03 | ▲ +57.15 after sell → book $10,471.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 60 | $21.75 | $2.19 | $+61.64 | $4,043.84 | ▲ +61.64 after sell → book $10,469.07; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 216 | $5.67 | $2.83 | $-27.22 | $5,265.72 | ▼ -27.22 after sell → book $10,466.23; vs 09:30 mark -2.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $6,597.23 | ▲ +92.64 after sell → book $10,464.03; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $7,946.24 | ▲ +102.43 after sell → book $10,461.90; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 714 | $1.79 | $9.34 | $+10.01 | $9,214.96 | ▲ +10.01 after sell → book $10,452.56; vs 09:30 mark -9.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,450.52 | ▲ +77.23 after sell → book $10,450.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,254.20 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1306.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $7,961.99 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1306.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,662.18 | — | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1306.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,357.63 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1306.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 528 | $2.47 | $6.81 | — | $4,046.66 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1306.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 676 | $1.93 | $8.72 | — | $2,733.26 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1306.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,477.08 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1306.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 989 | $1.32 | $12.76 | — | $158.85 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1306.32 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.85 | ▲ close $10,673.53 vs 09:30 $10,475.50 (session +261.93) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.85 | ▲ 09:30 equity $11,050.19 vs yday $10,673.53 (+376.66) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,361.91 | ▲ +6.74 after sell → book $11,048.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 75 | $16.57 | $2.24 | $-51.70 | $2,602.42 | ▼ -51.70 after sell → book $11,045.91; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,902.57 | ▲ +0.34 after sell → book $11,043.88; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 117 | $13.33 | $2.37 | $+252.69 | $5,459.81 | ▲ +252.69 after sell → book $11,041.51; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 528 | $2.40 | $6.91 | $-50.68 | $6,720.10 | ▼ -50.68 after sell → book $11,034.60; vs 09:30 mark -6.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 676 | $1.88 | $8.84 | $-51.36 | $7,982.14 | ▼ -51.36 after sell → book $11,025.76; vs 09:30 mark -8.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.75 | $2.07 | $-24.50 | $9,213.81 | ▼ -24.50 after sell → book $11,023.68; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 989 | $1.83 | $12.94 | $+478.70 | $11,010.75 | ▲ +478.70 after sell → book $11,010.75; vs 09:30 mark -12.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,010.75 | ▲ close $11,010.75 vs 09:30 $11,050.19 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,010.75 | ▲ 09:30 equity $11,010.75 vs yday $11,010.75 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 101 | $13.59 | $2.29 | — | $9,635.86 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1376.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 37 | $36.96 | $2.10 | — | $8,266.24 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1376.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 302 | $4.55 | $3.90 | — | $6,888.25 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1376.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 844 | $1.63 | $10.89 | — | $5,501.64 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1376.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 262 | $5.24 | $3.38 | — | $4,125.38 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1376.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 882 | $1.56 | $11.38 | — | $2,738.08 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1376.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2219 | $0.62 | $20.41 | — | $1,341.89 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1376.34 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 210 | $6.37 | $2.71 | — | $1.48 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1376.34 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.48 | ▲ close $11,011.66 vs 09:30 $11,010.75 (session +57.98) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.48 | ▼ 09:30 equity $10,906.05 vs yday $11,011.66 (-105.61) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `KURA` | 101 | $13.63 | $2.32 | $-0.57 | $1,375.79 | ▼ -0.57 after sell → book $10,903.73; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 37 | $38.24 | $2.12 | $+43.14 | $2,788.54 | ▲ +43.14 after sell → book $10,901.61; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZIP` | 302 | $4.31 | $3.96 | $-80.33 | $4,086.21 | ▼ -80.33 after sell → book $10,897.65; vs 09:30 mark -3.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 844 | $1.75 | $11.04 | $+83.57 | $5,556.39 | ▲ +83.57 after sell → book $10,886.61; vs 09:30 mark -11.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 262 | $4.98 | $3.43 | $-74.93 | $6,857.71 | ▼ -74.93 after sell → book $10,883.18; vs 09:30 mark -3.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 882 | $1.60 | $11.54 | $+12.37 | $8,257.38 | ▲ +12.37 after sell → book $10,871.64; vs 09:30 mark -11.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DEFT` | 2219 | $0.60 | $20.31 | $-89.54 | $9,564.04 | ▼ -89.54 after sell → book $10,851.34; vs 09:30 mark -20.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 210 | $6.13 | $2.75 | $-55.86 | $10,848.58 | ▼ -55.86 after sell → book $10,848.58; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `SLQT` | 9304 | $0.58 | $82.15 | — | $5,342.19 | — | combo gate; gate vol=good,ab=good; list yday_mover; 🔵; ret5=-27.5; leftover $5424.29 | — |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 43 | $121.87 | $2.12 | — | $99.67 | — | combo gate; gate vol=good,ab=good; list yday_mover; 🔵; ret5=-35.1; leftover $5424.29 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.67 | ▲ close $10,792.25 vs 09:30 $10,906.05 (session +27.94) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.67 | ▼ 09:30 equity $10,566.18 vs yday $10,792.25 (-226.07) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.67 | ▲ close $10,789.94 vs 09:30 $10,566.18 (session +223.76) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.67 | ▼ 09:30 equity $10,750.49 vs yday $10,789.94 (-39.45) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `SLQT` | 9304 | $0.53 | $78.90 | $-644.86 | $4,961.19 | ▼ -644.86 after sell → book $10,671.59; vs 09:30 mark -78.90 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 43 | $132.80 | $2.17 | $+465.70 | $10,669.41 | ▲ +465.70 after sell → book $10,669.41; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $9,396.66 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1333.68 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $8,080.01 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1333.68 | — |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 73 | $18.15 | $2.21 | — | $6,752.85 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+14.1; leftover $1333.68 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $5,474.99 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1333.68 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 71 | $18.75 | $2.20 | — | $4,141.54 | — | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=-5.0; leftover $1333.68 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRDL` | 647 | $2.06 | $8.35 | — | $2,800.37 | — | combo gate; gate vol=good,ab=good; list yday_gainer; ret5=+9.3; leftover $1333.68 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 57 | $23.30 | $2.16 | — | $1,470.11 | — | combo gate; gate vol=good,ab=good; list ohlc_hot; 🔵; ret5=+14.5; leftover $1333.68 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 70 | $19.00 | $2.20 | — | $137.91 | — | combo gate; gate vol=good,ab=good; list ohlc_hot; ret5=+7.5; leftover $1333.68 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.91 | ▼ close $10,479.79 vs 09:30 $10,750.49 (session -166.43) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.91 | ▼ 09:30 equity $10,409.38 vs yday $10,479.79 (-70.41) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $1,422.90 | ▲ +12.22 after sell → book $10,407.33; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $2,753.13 | ▲ +13.59 after sell → book $10,405.29; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 73 | $17.70 | $2.23 | $-37.29 | $4,043.00 | ▼ -37.29 after sell → book $10,403.06; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $5,231.66 | ▼ -89.19 after sell → book $10,401.02; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 71 | $19.25 | $2.23 | $+31.07 | $6,596.18 | ▲ +31.07 after sell → book $10,398.79; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRDL` | 647 | $1.92 | $8.46 | $-107.39 | $7,829.96 | ▼ -107.39 after sell → book $10,390.33; vs 09:30 mark -8.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 57 | $22.66 | $2.18 | $-40.82 | $9,119.40 | ▼ -40.82 after sell → book $10,388.15; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,119.40 | ▲ close $10,415.80 vs 09:30 $10,409.38 (session +27.65) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,119.40 | ▼ 09:30 equity $10,410.90 vs yday $10,415.80 (-4.90) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `TH` | 70 | $18.45 | $2.22 | $-42.92 | $10,408.68 | ▼ -42.92 after sell → book $10,408.68; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,408.68 | ▲ close $10,408.68 vs 09:30 $10,410.90 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,408.68 | ▲ 09:30 equity $10,408.68 vs yday $10,408.68 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,408.68 | ▲ close $10,408.68 vs 09:30 $10,408.68 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,408.68 | ▲ 09:30 equity $10,408.68 vs yday $10,408.68 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $9,214.61 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1301.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 84 | $15.45 | $2.24 | — | $7,914.57 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1301.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $6,744.99 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1301.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 77 | $16.77 | $2.22 | — | $5,451.48 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1301.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 23 | $55.42 | $2.06 | — | $4,174.76 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; ret5=-25.9; leftover $1301.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 596 | $2.18 | $7.69 | — | $2,867.79 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1301.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 54 | $23.88 | $2.15 | — | $1,576.12 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1301.08 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 124 | $10.42 | $2.36 | — | $281.68 | — | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1301.08 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $281.68 | ▼ close $10,254.01 vs 09:30 $10,408.68 (session -131.91) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $281.68 | ▲ 09:30 equity $10,302.77 vs yday $10,254.01 (+48.76) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $1,449.91 | ▼ -25.83 after sell → book $10,300.73; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 84 | $15.00 | $2.27 | $-42.31 | $2,707.65 | ▼ -42.31 after sell → book $10,298.47; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $153.62 | $2.03 | $+57.35 | $3,934.57 | ▲ +57.35 after sell → book $10,296.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 77 | $15.61 | $2.24 | $-93.78 | $5,134.30 | ▼ -93.78 after sell → book $10,294.19; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 23 | $55.79 | $2.08 | $+4.37 | $6,415.39 | ▲ +4.37 after sell → book $10,292.11; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 596 | $2.16 | $7.80 | $-27.41 | $7,694.95 | ▼ -27.41 after sell → book $10,284.31; vs 09:30 mark -7.80 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 54 | $23.84 | $2.17 | $-6.48 | $8,980.14 | ▼ -6.48 after sell → book $10,282.14; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 124 | $10.50 | $2.39 | $+5.16 | $10,279.75 | ▲ +5.16 after sell → book $10,279.75; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 371 | $3.46 | $4.79 | — | $8,991.30 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1284.97 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 509 | $2.52 | $6.57 | — | $7,702.06 | — | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1284.97 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 191 | $6.71 | $2.56 | — | $6,417.88 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1284.97 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 676 | $1.90 | $8.72 | — | $5,124.76 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1284.97 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 268 | $4.78 | $3.46 | — | $3,840.27 | — | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1284.97 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 113 | $11.31 | $2.33 | — | $2,559.91 | — | combo gate; gate vol=good,ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1284.97 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $1,530.35 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1284.97 | — |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 45 | $28.00 | $2.12 | — | $268.23 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+8.7; leftover $1284.97 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $268.23 | ▼ close $10,197.65 vs 09:30 $10,302.77 (session -49.56) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $268.23 | ▼ 09:30 equity $10,082.96 vs yday $10,197.65 (-114.69) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 371 | $3.43 | $4.86 | $-20.77 | $1,535.90 | ▼ -20.77 after sell → book $10,078.10; vs 09:30 mark -4.86 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 509 | $2.38 | $6.66 | $-84.49 | $2,740.66 | ▼ -84.49 after sell → book $10,071.44; vs 09:30 mark -6.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 191 | $6.57 | $2.60 | $-31.91 | $3,992.92 | ▼ -31.91 after sell → book $10,068.83; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 676 | $2.00 | $8.84 | $+50.04 | $5,336.08 | ▲ +50.04 after sell → book $10,059.99; vs 09:30 mark -8.84 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 268 | $4.30 | $3.51 | $-135.61 | $6,484.97 | ▼ -135.61 after sell → book $10,056.48; vs 09:30 mark -3.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 113 | $11.22 | $2.36 | $-14.86 | $7,750.47 | ▼ -14.86 after sell → book $10,054.12; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $8,790.75 | ▲ +10.73 after sell → book $10,052.10; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MLYS` | 45 | $28.03 | $2.15 | $-2.92 | $10,049.96 | ▼ -2.92 after sell → book $10,049.96; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.96 | ▲ close $10,049.96 vs 09:30 $10,082.96 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.96 | ▲ 09:30 equity $10,049.96 vs yday $10,049.96 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.96 | ▲ close $10,049.96 vs 09:30 $10,049.96 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.96 | ▲ 09:30 equity $10,049.96 vs yday $10,049.96 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.96 | ▲ close $10,049.96 vs 09:30 $10,049.96 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.96 | ▲ 09:30 equity $10,049.96 vs yday $10,049.96 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 7 | $164.43 | $2.01 | — | $8,896.94 | — | combo gate; gate vol=good,ab=good; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1256.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 16 | $77.33 | $2.04 | — | $7,657.62 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; ret5=+2.5; leftover $1256.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 465 | $2.70 | $6.00 | — | $6,396.12 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1256.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 401 | $3.13 | $5.17 | — | $5,135.82 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+24.2; leftover $1256.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 114 | $10.95 | $2.33 | — | $3,885.19 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1256.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 255 | $4.91 | $3.29 | — | $2,629.85 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1256.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 14 | $84.27 | $2.03 | — | $1,448.03 | — | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1256.24 | — |
| 2026-09-11 09:30 ET | **BUY** | `ANGX` | 233 | $5.38 | $3.01 | — | $191.49 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+19.8; leftover $1256.24 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $191.49 | ▲ close $10,071.39 vs 09:30 $10,049.96 (session +47.32) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.49 | ▲ 09:30 equity $10,082.90 vs yday $10,071.39 (+11.51) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 7 | $141.42 | $2.03 | $-165.11 | $1,179.40 | ▼ -165.11 after sell → book $10,080.87; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIST` | 16 | $77.10 | $2.06 | $-7.78 | $2,410.94 | ▼ -7.78 after sell → book $10,078.81; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CMRC` | 401 | $3.51 | $5.25 | $+141.96 | $3,813.20 | ▲ +141.96 after sell → book $10,073.56; vs 09:30 mark -5.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 114 | $10.29 | $2.36 | $-79.93 | $4,983.90 | ▼ -79.93 after sell → book $10,071.20; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 255 | $5.03 | $3.34 | $+23.97 | $6,263.21 | ▲ +23.97 after sell → book $10,067.86; vs 09:30 mark -3.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 14 | $86.06 | $2.05 | $+20.98 | $7,465.99 | ▲ +20.98 after sell → book $10,065.80; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ANGX` | 233 | $5.57 | $3.05 | $+38.21 | $8,760.75 | ▲ +38.21 after sell → book $10,062.75; vs 09:30 mark -3.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,760.75 | ▲ close $10,220.85 vs 09:30 $10,082.90 (session +158.10) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,760.75 | ▲ 09:30 equity $10,341.75 vs yday $10,220.85 (+120.90) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `INDP` | 465 | $3.40 | $6.09 | $+313.41 | $10,335.66 | ▲ +313.41 after sell → book $10,335.66; vs 09:30 mark -6.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,335.66 | ▲ close $10,335.66 vs 09:30 $10,341.75 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,335.66 | ▲ 09:30 equity $10,335.66 vs yday $10,335.66 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 16 | $77.12 | $2.04 | — | $9,099.70 | — | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot; ret5=+7.2; leftover $1291.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 220 | $5.87 | $2.84 | — | $7,805.46 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1291.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 14 | $87.40 | $2.03 | — | $6,579.83 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1291.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 47 | $27.09 | $2.13 | — | $5,304.47 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1291.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 14 | $89.38 | $2.03 | — | $4,051.12 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1291.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 55 | $23.29 | $2.15 | — | $2,768.01 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=+16.1; leftover $1291.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 38 | $33.14 | $2.10 | — | $1,506.59 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=-2.9; leftover $1291.96 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 45 | $28.16 | $2.12 | — | $237.27 | — | combo gate; gate vol=good,ab=good; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1291.96 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $237.27 | ▼ close $10,232.10 vs 09:30 $10,335.66 (session -86.11) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $237.27 | ▲ 09:30 equity $10,402.76 vs yday $10,232.10 (+170.66) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 16 | $76.44 | $2.06 | $-14.98 | $1,458.25 | ▼ -14.98 after sell → book $10,400.70; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 220 | $5.58 | $2.88 | $-69.52 | $2,682.96 | ▼ -69.52 after sell → book $10,397.82; vs 09:30 mark -2.88 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 14 | $83.20 | $2.05 | $-62.88 | $3,845.71 | ▼ -62.88 after sell → book $10,395.77; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 47 | $28.23 | $2.15 | $+49.30 | $5,170.37 | ▲ +49.30 after sell → book $10,393.61; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 14 | $86.76 | $2.05 | $-40.76 | $6,382.96 | ▼ -40.76 after sell → book $10,391.56; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 55 | $24.09 | $2.18 | $+39.67 | $7,705.73 | ▲ +39.67 after sell → book $10,389.39; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 45 | $28.59 | $2.15 | $+15.30 | $8,990.36 | ▲ +15.30 after sell → book $10,387.24; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 5 | $233.85 | $2.00 | — | $7,819.11 | — | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot; ret5=+11.7; leftover $1284.34 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $6,636.21 | — | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot; ret5=+17.7; leftover $1284.34 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 169 | $7.59 | $2.50 | — | $5,351.01 | — | combo gate; gate vol=good,ab=good; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1284.34 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 49 | $25.95 | $2.14 | — | $4,077.32 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1284.34 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 7 | $170.85 | $2.01 | — | $2,879.36 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1284.34 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 535 | $2.40 | $6.90 | — | $1,588.46 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $1284.34 | — |
| 2026-09-17 09:30 ET | **BUY** | `CYPH` | 480 | $2.67 | $6.19 | — | $298.26 | — | combo gate; gate vol=good,ab=good; list yday_gainer; 🔵; ret5=-0.4; leftover $1284.34 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $298.26 | ▲ close $10,732.98 vs 09:30 $10,402.76 (session +369.50) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $298.26 | ▲ 09:30 equity $10,804.65 vs yday $10,732.98 (+71.67) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 38 | $39.50 | $2.13 | $+237.45 | $1,797.14 | ▲ +237.45 after sell → book $10,802.53; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 5 | $249.13 | $2.02 | $+72.37 | $3,040.76 | ▲ +72.37 after sell → book $10,800.50; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $4,210.73 | ▼ -12.93 after sell → book $10,798.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 169 | $7.98 | $2.54 | $+60.88 | $5,556.81 | ▲ +60.88 after sell → book $10,795.93; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 49 | $26.14 | $2.16 | $+5.02 | $6,835.52 | ▲ +5.02 after sell → book $10,793.78; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 7 | $182.33 | $2.03 | $+76.32 | $8,109.79 | ▲ +76.32 after sell → book $10,791.74; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SABR` | 535 | $2.29 | $7.00 | $-72.75 | $9,327.94 | ▼ -72.75 after sell → book $10,784.74; vs 09:30 mark -7.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 6 | $209.52 | $2.01 | — | $8,068.82 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1332.56 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 6 | $219.62 | $2.01 | — | $6,749.09 | — | combo gate; gate vol=good,ab=good; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1332.56 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 15 | $85.00 | $2.04 | — | $5,472.05 | — | combo gate; gate vol=good,ab=good; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1332.56 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 90 | $14.79 | $2.26 | — | $4,138.69 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1332.56 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 45 | $29.32 | $2.12 | — | $2,817.17 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1332.56 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 117 | $11.38 | $2.34 | — | $1,483.37 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+19.5; leftover $1332.56 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 16 | $81.40 | $2.04 | — | $178.93 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $1332.56 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.93 | ▲ close $11,022.21 vs 09:30 $10,804.65 (session +252.28) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.93 | ▲ 09:30 equity $11,294.56 vs yday $11,022.21 (+272.35) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 480 | $4.00 | $6.29 | $+623.52 | $2,092.64 | ▲ +623.52 after sell → book $11,288.27; vs 09:30 mark -6.29 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 6 | $210.00 | $2.03 | $-1.16 | $3,350.61 | ▼ -1.16 after sell → book $11,286.24; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 6 | $230.25 | $2.03 | $+59.74 | $4,730.09 | ▲ +59.74 after sell → book $11,284.22; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 15 | $82.83 | $2.06 | $-36.64 | $5,970.48 | ▼ -36.64 after sell → book $11,282.16; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 90 | $14.58 | $2.29 | $-23.45 | $7,280.40 | ▼ -23.45 after sell → book $11,279.88; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 45 | $29.43 | $2.15 | $+0.68 | $8,602.60 | ▲ +0.68 after sell → book $11,277.73; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VITL` | 117 | $12.05 | $2.37 | $+73.68 | $10,010.08 | ▲ +73.68 after sell → book $11,275.36; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 16 | $79.08 | $2.06 | $-41.22 | $11,273.30 | ▼ -41.22 after sell → book $11,273.30; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 8 | $157.87 | $2.01 | — | $10,008.33 | — | combo gate; gate vol=good,ab=good; list flatten; ret5=+6.5; leftover $1409.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 15 | $88.83 | $2.04 | — | $8,673.84 | — | combo gate; gate vol=good,ab=good; list flatten; ret5=+7.6; leftover $1409.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 151 | $9.31 | $2.44 | — | $7,265.59 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1409.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 104 | $13.47 | $2.30 | — | $5,861.89 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1409.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 107 | $13.05 | $2.31 | — | $4,463.22 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1409.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 120 | $11.67 | $2.35 | — | $3,060.47 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+31.3; leftover $1409.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 570 | $2.47 | $7.35 | — | $1,645.22 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+73.6; leftover $1409.16 | — |
| 2026-09-21 09:30 ET | **BUY** | `CAN` | 3371 | $0.42 | $24.20 | — | $211.94 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+10.7; leftover $1409.16 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $211.94 | ▲ close $11,379.22 vs 09:30 $11,294.56 (session +150.93) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $211.94 | ▼ 09:30 equity $11,315.69 vs yday $11,379.22 (-63.53) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 107 | $12.99 | $2.34 | $-11.07 | $1,599.53 | ▼ -11.07 after sell → book $11,313.35; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SECZ` | 120 | $12.96 | $2.38 | $+150.07 | $3,152.35 | ▲ +150.07 after sell → book $11,310.97; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `CAN` | 3371 | $0.41 | $24.34 | $-92.36 | $4,493.27 | ▼ -92.36 after sell → book $11,286.64; vs 09:30 mark -24.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 968 | $0.58 | $8.52 | — | $3,923.31 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $561.66 | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 61 | $9.11 | $2.17 | — | $3,365.43 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+44.4; leftover $561.66 | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 1 | $319.41 | $1.99 | — | $3,044.02 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+35.1; leftover $561.66 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 20 | $28.02 | $2.05 | — | $2,481.57 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $561.66 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,481.57 | ▼ close $11,163.72 vs 09:30 $11,315.69 (session -108.18) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,481.57 | ▲ 09:30 equity $11,508.82 vs yday $11,163.72 (+345.10) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 151 | $9.50 | $2.48 | $+23.77 | $3,913.59 | ▲ +23.77 after sell → book $11,506.34; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 104 | $12.84 | $2.33 | $-70.67 | $5,246.62 | ▼ -70.67 after sell → book $11,504.01; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DEFT` | 968 | $0.57 | $8.64 | $-22.00 | $5,794.58 | ▼ -22.00 after sell → book $11,495.37; vs 09:30 mark -8.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 61 | $8.39 | $2.19 | $-48.29 | $6,304.18 | ▼ -48.29 after sell → book $11,493.18; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 1 | $331.78 | $2.01 | $+8.36 | $6,633.95 | ▲ +8.36 after sell → book $11,491.17; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 20 | $25.90 | $2.07 | $-46.52 | $7,149.88 | ▼ -46.52 after sell → book $11,489.10; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 69 | $20.65 | $2.20 | — | $5,722.83 | — | combo gate; gate vol=good,ab=good; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1429.98 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 12 | $116.00 | $2.03 | — | $4,328.80 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $1429.98 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 144 | $9.90 | $2.42 | — | $2,900.78 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $1429.98 | — |
| 2026-09-23 09:30 ET | **BUY** | `VICR` | 5 | $266.50 | $2.00 | — | $1,566.28 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.2; leftover $1429.98 | — |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 20 | $70.84 | $2.05 | — | $147.43 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $1429.98 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $147.43 | ▼ close $11,275.24 vs 09:30 $11,508.82 (session -203.16) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $147.43 | ▼ 09:30 equity $11,161.92 vs yday $11,275.24 (-113.32) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 8 | $163.95 | $2.03 | $+44.59 | $1,456.99 | ▲ +44.59 after sell → book $11,159.89; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 15 | $87.67 | $2.06 | $-21.42 | $2,770.06 | ▼ -21.42 after sell → book $11,157.83; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 570 | $2.68 | $7.46 | $+104.89 | $4,290.20 | ▲ +104.89 after sell → book $11,150.37; vs 09:30 mark -7.46 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 69 | $20.52 | $2.22 | $-13.39 | $5,703.86 | ▼ -13.39 after sell → book $11,148.15; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLLN` | 12 | $112.33 | $2.05 | $-48.11 | $7,049.77 | ▼ -48.11 after sell → book $11,146.10; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 144 | $9.12 | $2.46 | $-117.20 | $8,360.60 | ▼ -117.20 after sell → book $11,143.65; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 5 | $274.61 | $2.03 | $+36.52 | $9,731.62 | ▲ +36.52 after sell → book $11,141.62; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INOD` | 20 | $70.50 | $2.07 | $-10.92 | $11,139.55 | ▼ -10.92 after sell → book $11,139.55; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,139.55 | ▲ close $11,139.55 vs 09:30 $11,161.92 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,290.83 | ▲ 09:30 equity $9,290.83 vs yday $9,290.83 (+0.00) | 09:30 open · cash $9,290.83 · no holdings · equity $9,290.83 vs prior close $9,290.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 44 | $26.27 | $2.12 | — | $8,132.83 | — | combo gate; gate vol=good,ab=good; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1161.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 64 | $17.91 | $2.18 | — | $6,984.41 | — | combo gate; gate vol=good,ab=good; list probable; 🔵; ret5=+3.7; leftover $1161.35 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 13 | $83.69 | $2.03 | — | $5,894.34 | — | combo gate; gate vol=good,ab=good; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1161.35 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GLND` | 191 | $6.06 | $2.56 | — | $4,734.32 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; ret5=+342.1; leftover $1161.35 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 300 | $3.86 | $3.87 | — | $3,572.45 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1161.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 113 | $10.20 | $2.33 | — | $2,417.52 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1161.35 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 6 | $184.00 | $2.01 | — | $1,311.51 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $1161.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 9 | $123.50 | $2.02 | — | $197.99 | — | combo gate; gate vol=good,ab=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1161.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $197.99 | ▼ close $9,229.59 vs 09:30 $9,290.83 (session -42.11) | 16:00 close · cash $197.99 · equity $9,229.59 vs 09:30 $9,290.83 (-61.24; session marks -42.11) · 8 name(s) marked open→close (per-name table). WRBY×44 09:30 $26.27 → close $26.71 +19.36; PL×64 09:30 $17.91 → close $17.43 -30.72; TEM×13 09:30 $83.69 → close $85.01 +17.10; GLND×191 09:30 $6.06 → close $5.54 -99.32; ZSQR×300 09:30 $3.86 → close $3.78 -24.00; DNA×113 09:30 $10.20 → close $10.66 +51.98; TWST×6 09:30 $184.00 → close $182.83 -7.02; GRAL×9 09:30 $123.50 → close $126.89 +30.51 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $197.99 | ▼ 09:30 equity $9,045.33 vs yday $9,229.59 (-184.26) | 09:30 open · cash $197.99 (unchanged overnight, no fees) · equity $9,045.33 vs prior close $9,229.59 (-184.26) · 8 name(s) re-marked at the open (per-name table). DNA×113 yday $10.66 → 09:30 $10.38 -32.21; GLND×191 yday $5.54 → 09:30 $5.14 -76.78; GRAL×9 yday $126.89 → 09:30 $128.90 +18.09; PL×64 yday $17.43 → 09:30 $17.17 -16.64; TEM×13 yday $85.01 → 09:30 $83.57 -18.72; TWST×6 yday $182.83 → 09:30 $181.87 -5.76; WRBY×44 yday $26.71 → 09:30 $26.00 -31.24; ZSQR×300 yday $3.78 → 09:30 $3.71 -21.00 | — |
| 2026-09-28 09:30 ET | **SELL** | `DNA` | 113 | $10.38 | $2.36 | $+15.09 | $1,368.01 | ▲ +15.09 after sell → book $9,042.98; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `GLND` | 191 | $5.14 | $2.60 | $-181.27 | $2,346.76 | ▼ -181.27 after sell → book $9,040.37; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `GRAL` | 9 | $128.90 | $2.04 | $+44.55 | $3,504.82 | ▲ +44.55 after sell → book $9,038.33; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `PL` | 64 | $17.17 | $2.20 | $-51.74 | $4,601.50 | ▼ -51.74 after sell → book $9,036.13; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `TEM` | 13 | $83.57 | $2.05 | $-5.70 | $5,685.86 | ▼ -5.70 after sell → book $9,034.08; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-28 09:30 ET | **SELL** | `TWST` | 6 | $181.87 | $2.03 | $-16.82 | $6,775.05 | ▼ -16.82 after sell → book $9,032.05; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `WRBY` | 44 | $26.00 | $2.14 | $-16.14 | $7,916.91 | ▼ -16.14 after sell → book $9,029.91; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `ZSQR` | 300 | $3.71 | $3.93 | $-52.80 | $9,025.98 | ▼ -52.80 after sell → book $9,025.98; vs 09:30 mark -3.93 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,025.98 | ▲ close $9,025.98 vs 09:30 $9,045.33 (session +0.00) | 16:00 close · cash $9,025.98 · no lots left · equity $9,025.98. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,025.98 | ▲ 09:30 equity $9,025.98 vs yday $9,025.98 (+0.00) | 09:30 open · cash $9,025.98 · no holdings · equity $9,025.98 vs prior close $9,025.98 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-29 09:30 ET | **BUY** | `KOD` | 12 | $87.50 | $2.03 | — | $7,973.95 | — | combo gate; gate ab=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+177.4; leftover $1128.25 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `AMC` | 339 | $3.32 | $4.37 | — | $6,844.10 | — | combo gate; gate ab=good,vol=good; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.8; leftover $1128.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `XERS` | 115 | $9.77 | $2.33 | — | $5,718.22 | — | combo gate; gate ab=good,vol=good; list yday_gainer,ohlc_hot; ret5=+11.7; leftover $1128.25 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `BB` | 127 | $8.86 | $2.37 | — | $4,590.62 | — | combo gate; gate ab=good,vol=good; list yday_gainer; ⚪; ret5=+3.2; leftover $1128.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `CYPH` | 337 | $3.34 | $4.35 | — | $3,460.70 | — | combo gate; gate ab=good,vol=good; list yday_mover; ret5=-2.6; leftover $1128.25 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `GLND` | 283 | $3.98 | $3.65 | — | $2,330.71 | — | combo gate; gate ab=good,vol=good; list yday_mover; ret5=+55.9; leftover $1128.25 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `MDB` | 3 | $335.14 | $2.00 | — | $1,323.27 | — | combo gate; gate ab=good,vol=good; list yday_mover; ⚪; ret5=-17.8; leftover $1128.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `LTRX` | 153 | $7.34 | $2.45 | — | $197.80 | — | combo gate; gate ab=good,vol=good; list ohlc_hot; ret5=+16.8; leftover $1128.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $197.80 | ▲ close $9,139.49 vs 09:30 $9,025.98 (session +137.07) | 16:00 close · cash $197.80 · equity $9,139.49 vs 09:30 $9,025.98 (+113.51; session marks +137.07) · 8 name(s) marked open→close (per-name table). KOD×12 09:30 $87.50 → close $91.12 +43.44; AMC×339 09:30 $3.32 → close $3.08 -81.36; XERS×115 09:30 $9.77 → close $9.98 +24.15; BB×127 09:30 $8.86 → close $8.72 -17.78; CYPH×337 09:30 $3.34 → close $3.23 -37.07; GLND×283 09:30 $3.98 → close $4.75 +217.91; MDB×3 09:30 $335.14 → close $337.19 +6.14; LTRX×153 09:30 $7.34 → close $7.22 -18.36 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $197.80 | ▲ 09:30 equity $9,139.49 vs yday $9,139.49 (+0.00) | 09:30 open · cash $197.80 (unchanged overnight, no fees) · equity $9,139.49 vs prior close $9,139.49 (+0.00) · 8 name(s) re-marked at the open (per-name table). AMC×339 yday $3.08 → 09:30 $3.08 +0.00; BB×127 yday $8.72 → 09:30 $8.72 +0.00; CYPH×337 yday $3.23 → 09:30 $3.23 +0.00; GLND×283 yday $4.75 → 09:30 $4.75 +0.00; KOD×12 yday $91.12 → 09:30 $91.12 +0.00; LTRX×153 yday $7.22 → 09:30 $7.22 +0.00; MDB×3 yday $337.19 → 09:30 $337.19 +0.00; XERS×115 yday $9.98 → 09:30 $9.98 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $197.80 | ▲ close $9,139.49 vs 09:30 $9,139.49 (session +0.00) | 16:00 close · cash $197.80 · equity $9,139.49 vs 09:30 $9,139.49 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). AMC×339 09:30 $3.08 → close $3.08 +0.00; BB×127 09:30 $8.72 → close $8.72 +0.00; CYPH×337 09:30 $3.23 → close $3.23 +0.00; GLND×283 09:30 $4.75 → close $4.75 +0.00; KOD×12 09:30 $91.12 → close $91.12 +0.00; LTRX×153 09:30 $7.22 → close $7.22 +0.00; MDB×3 09:30 $337.19 → close $337.19 +0.00; XERS×115 09:30 $9.98 → close $9.98 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $197.80 | ▲ 09:30 equity $9,240.38 vs yday $9,139.49 (+100.89) | 09:30 open · cash $197.80 (unchanged overnight, no fees) · equity $9,240.38 vs prior close $9,139.49 (+100.89) · 8 name(s) re-marked at the open (per-name table). AMC×339 yday $3.08 → 09:30 $2.98 -33.90; BB×127 yday $8.72 → 09:30 $8.97 +31.75; CYPH×337 yday $3.23 → 09:30 $3.13 -33.70; GLND×283 yday $4.75 → 09:30 $5.06 +87.73; KOD×12 yday $91.12 → 09:30 $95.41 +51.48; LTRX×153 yday $7.22 → 09:30 $7.25 +4.59; MDB×3 yday $337.19 → 09:30 $352.47 +45.84; XERS×115 yday $9.98 → 09:30 $9.52 -52.90 | — |
| 2026-10-01 09:30 ET | **SELL** | `AMC` | 339 | $2.98 | $4.44 | $-124.07 | $1,203.58 | ▼ -124.07 after sell → book $9,235.94; vs 09:30 mark -4.44 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `BB` | 127 | $8.97 | $2.40 | $+9.20 | $2,340.37 | ▲ +9.20 after sell → book $9,233.54; vs 09:30 mark -2.40 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `CYPH` | 337 | $3.13 | $4.41 | $-79.53 | $3,390.77 | ▼ -79.53 after sell → book $9,229.13; vs 09:30 mark -4.41 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `GLND` | 283 | $5.06 | $3.71 | $+298.28 | $4,819.04 | ▲ +298.28 after sell → book $9,225.42; vs 09:30 mark -3.71 | exit unpriced hold on first bar after 2 sess | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟡 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `KOD` | 12 | $95.41 | $2.05 | $+90.85 | $5,961.91 | ▲ +90.85 after sell → book $9,223.37; vs 09:30 mark -2.05 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `LTRX` | 153 | $7.25 | $2.48 | $-18.70 | $7,068.68 | ▼ -18.70 after sell → book $9,220.89; vs 09:30 mark -2.48 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `MDB` | 3 | $352.47 | $2.02 | $+47.96 | $8,124.07 | ▲ +47.96 after sell → book $9,218.87; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `XERS` | 115 | $9.52 | $2.36 | $-33.45 | $9,216.50 | ▼ -33.45 after sell → book $9,216.50; vs 09:30 mark -2.37 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `AVPT` | 80 | $14.27 | $2.23 | — | $8,072.67 | — | combo gate; gate ab=good,vol=good; list flatten,ohlc_hot; 🔵; ret5=+7.3; leftover $1152.06 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `CDNL` | 38 | $30.30 | $2.10 | — | $6,919.17 | — | combo gate; gate ab=good,vol=good; list probable,yday_gainer; ret5=+6.7; leftover $1152.06 | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🔴 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PMVP` | 685 | $1.68 | $8.84 | — | $5,759.53 | — | combo gate; gate ab=good,vol=good; list yday_gainer,yday_mover; ret5=+21.9; leftover $1152.06 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `UTHR` | 2 | $557.53 | $2.00 | — | $4,642.48 | — | combo gate; gate ab=good,vol=good; list yday_gainer,yday_mover; ret5=+10.7; leftover $1152.06 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `QSI` | 960 | $1.20 | $12.38 | — | $3,478.09 | — | combo gate; gate ab=good,vol=good; list yday_gainer; ret5=+41.0; leftover $1152.06 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `CAPR` | 123 | $9.36 | $2.36 | — | $2,324.45 | — | combo gate; gate ab=good,vol=good; list yday_gainer; 🔵; ret5=+9.2; leftover $1152.06 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `FORM` | 7 | $149.45 | $2.01 | — | $1,276.29 | — | combo gate; gate ab=good,vol=good; list yday_gainer,ohlc_hot; ret5=+13.8; leftover $1152.06 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `OMDA` | 55 | $20.90 | $2.15 | — | $124.64 | — | combo gate; gate ab=good,vol=good; list yday_gainer; ret5=+0.6; leftover $1152.06 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.64 | ▲ close $9,356.72 vs 09:30 $9,240.38 (session +174.29) | 16:00 close · cash $124.64 · equity $9,356.72 vs 09:30 $9,240.38 (+116.34; session marks +174.29) · 8 name(s) marked open→close (per-name table). AVPT×80 09:30 $14.27 → close $14.08 -15.20; CDNL×38 09:30 $30.30 → close $29.74 -21.28; PMVP×685 09:30 $1.68 → close $1.75 +47.95; UTHR×2 09:30 $557.53 → close $571.38 +27.70; QSI×960 09:30 $1.20 → close $1.43 +220.80; CAPR×123 09:30 $9.36 → close $8.94 -51.66; FORM×7 09:30 $149.45 → close $148.44 -7.07; OMDA×55 09:30 $20.90 → close $20.41 -26.95 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.64 | ▲ 09:30 equity $9,395.23 vs yday $9,356.72 (+38.51) | 09:30 open · cash $124.64 (unchanged overnight, no fees) · equity $9,395.23 vs prior close $9,356.72 (+38.51) · 8 name(s) re-marked at the open (per-name table). AVPT×80 yday $14.08 → 09:30 $14.22 +11.20; CAPR×123 yday $8.94 → 09:30 $9.14 +24.60; CDNL×38 yday $29.74 → 09:30 $30.29 +20.90; FORM×7 yday $148.44 → 09:30 $150.50 +14.42; OMDA×55 yday $20.41 → 09:30 $20.74 +18.15; PMVP×685 yday $1.75 → 09:30 $1.75 +0.00; QSI×960 yday $1.43 → 09:30 $1.38 -48.00; UTHR×2 yday $571.38 → 09:30 $570.00 -2.76 | — |
| 2026-10-02 09:30 ET | **SELL** | `AVPT` | 80 | $14.22 | $2.25 | $-8.48 | $1,259.99 | ▼ -8.48 after sell → book $9,392.98; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `CAPR` | 123 | $9.14 | $2.39 | $-31.81 | $2,381.82 | ▼ -31.81 after sell → book $9,390.59; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `CDNL` | 38 | $30.29 | $2.12 | $-4.61 | $3,530.71 | ▼ -4.61 after sell → book $9,388.46; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `FORM` | 7 | $150.50 | $2.03 | $+3.31 | $4,582.18 | ▲ +3.31 after sell → book $9,386.43; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `OMDA` | 55 | $20.74 | $2.17 | $-13.13 | $5,720.71 | ▼ -13.13 after sell → book $9,384.26; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `PMVP` | 685 | $1.75 | $8.96 | $+30.15 | $6,910.50 | ▲ +30.15 after sell → book $9,375.30; vs 09:30 mark -8.96 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **SELL** | `UTHR` | 2 | $570.00 | $2.02 | $+20.93 | $8,048.48 | ▲ +20.93 after sell → book $9,373.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 09:30 ET | **BUY** | `CDNA` | 17 | $66.33 | $2.04 | — | $6,918.83 | — | combo gate; gate ab=good,vol=good; list flatten,ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $1149.78 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ETON` | 21 | $52.42 | $2.05 | — | $5,815.96 | — | combo gate; gate ab=good,vol=good; list flatten; 🔵; ⚪; ret5=-12.6; leftover $1149.78 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `COHR` | 3 | $316.56 | $2.00 | — | $4,864.26 | — | combo gate; gate ab=good,vol=good; list probable,yday_gainer; 🔵; ret5=+9.8; leftover $1149.78 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `INOD` | 15 | $73.05 | $2.04 | — | $3,766.48 | — | combo gate; gate ab=good,vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=-0.0; leftover $1149.78 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `MAT` | 75 | $15.13 | $2.21 | — | $2,629.14 | — | combo gate; gate ab=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+14.5; leftover $1149.78 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ACN` | 5 | $211.02 | $2.00 | — | $1,572.03 | — | combo gate; gate ab=good,vol=good; list yday_gainer,yday_mover; ret5=+19.7; leftover $1149.78 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `SNPS` | 2 | $497.86 | $2.00 | — | $574.33 | — | combo gate; gate ab=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+15.4; leftover $1149.78 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $574.33 | ▲ close $9,548.16 vs 09:30 $9,395.23 (session +189.22) | 16:00 close · cash $574.33 · equity $9,548.16 vs 09:30 $9,395.23 (+152.93; session marks +189.22) · 8 name(s) marked open→close (per-name table). QSI×960 09:30 $1.38 → close $1.55 +163.20; CDNA×17 09:30 $66.33 → close $67.15 +13.94; ETON×21 09:30 $52.42 → close $55.36 +61.74; COHR×3 09:30 $316.56 → close $337.04 +61.43; INOD×15 09:30 $73.05 → close $70.07 -44.70; MAT×75 09:30 $15.13 → close $15.27 +10.12; ACN×5 09:30 $211.02 → close $198.90 -60.60; SNPS×2 09:30 $497.86 → close $489.90 -15.91 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $574.33 | ▲ 09:30 equity $9,549.95 vs yday $9,548.16 (+1.80) | 09:30 open · cash $574.33 (unchanged overnight, no fees) · equity $9,549.95 vs prior close $9,548.16 (+1.80) · 8 name(s) re-marked at the open (per-name table). ACN×5 yday $198.90 → 09:30 $196.30 -13.00; CDNA×17 yday $67.15 → 09:30 $66.90 -4.25; COHR×3 yday $337.04 → 09:30 $340.93 +11.65; ETON×21 yday $55.36 → 09:30 $55.85 +10.29; INOD×15 yday $70.07 → 09:30 $70.98 +13.65; MAT×75 yday $15.27 → 09:30 $15.20 -5.25; QSI×960 yday $1.55 → 09:30 $1.52 -24.00; SNPS×2 yday $489.90 → 09:30 $496.25 +12.70 | — |
| 2026-10-05 09:30 ET | **SELL** | `ACN` | 5 | $196.30 | $2.02 | $-77.63 | $1,553.81 | ▼ -77.63 after sell → book $9,547.93; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `CDNA` | 17 | $66.90 | $2.06 | $+5.59 | $2,689.04 | ▲ +5.59 after sell → book $9,545.87; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `COHR` | 3 | $340.93 | $2.02 | $+69.06 | $3,709.80 | ▲ +69.06 after sell → book $9,543.85; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `ETON` | 21 | $55.85 | $2.07 | $+67.90 | $4,880.58 | ▲ +67.90 after sell → book $9,541.78; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `INOD` | 15 | $70.98 | $2.06 | $-35.14 | $5,943.22 | ▼ -35.14 after sell → book $9,539.72; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `MAT` | 75 | $15.20 | $2.24 | $+0.42 | $7,080.98 | ▲ +0.42 after sell → book $9,537.48; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `QSI` | 960 | $1.52 | $12.56 | $+287.06 | $8,532.43 | ▲ +287.06 after sell → book $9,524.93; vs 09:30 mark -12.55 | dropped from list after 2 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `SNPS` | 2 | $496.25 | $2.02 | $-7.22 | $9,522.91 | ▼ -7.22 after sell → book $9,522.91; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **BUY** | `FEAM` | 306 | $3.88 | $3.95 | — | $8,331.69 | — | combo gate; gate ab=good,vol=good; list yday_gainer,yday_mover; ret5=+38.6; leftover $1190.36 | join🟡 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `SYNA` | 9 | $120.98 | $2.02 | — | $7,240.85 | — | combo gate; gate ab=good,vol=good; list yday_gainer,yday_mover; ret5=+17.9; leftover $1190.36 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `WOLF` | 34 | $34.04 | $2.09 | — | $6,081.40 | — | combo gate; gate ab=good,vol=good; list yday_gainer,yday_mover; ret5=+28.0; leftover $1190.36 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `AZTA` | 29 | $39.72 | $2.08 | — | $4,927.44 | — | combo gate; gate ab=good,vol=good; list yday_gainer,yday_mover; ret5=+13.9; leftover $1190.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `VSH` | 31 | $37.64 | $2.08 | — | $3,758.52 | — | combo gate; gate ab=good,vol=good; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $1190.36 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `RXO` | 41 | $29.00 | $2.11 | — | $2,567.40 | — | combo gate; gate ab=good,vol=good; list yday_gainer; ret5=+18.1; leftover $1190.36 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `ALGM` | 30 | $39.56 | $2.08 | — | $1,378.52 | — | combo gate; gate ab=good,vol=good; list yday_gainer,ohlc_hot; ret5=+7.5; leftover $1190.36 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **BUY** | `VECO` | 20 | $56.94 | $2.05 | — | $237.67 | — | combo gate; gate ab=good,vol=good; list ohlc_hot; ret5=+15.5; leftover $1190.36 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $237.67 | ▼ close $9,498.70 vs 09:30 $9,549.95 (session -5.75) | 16:00 close · cash $237.67 · equity $9,498.70 vs 09:30 $9,549.95 (-51.25; session marks -5.75) · 8 name(s) marked open→close (per-name table). FEAM×306 09:30 $3.88 → close $3.92 +12.24; SYNA×9 09:30 $120.98 → close $119.73 -11.25; WOLF×34 09:30 $34.04 → close $33.53 -17.34; AZTA×29 09:30 $39.72 → close $42.73 +87.29; VSH×31 09:30 $37.64 → close $37.10 -16.74; RXO×41 09:30 $29.00 → close $28.65 -14.35; ALGM×30 09:30 $39.56 → close $38.46 -33.00; VECO×20 09:30 $56.94 → close $56.31 -12.60 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `QSI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SID` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `TLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ICLR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAIL` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `BRVE` | no_price | no 09:30 open |
| 2026-09-22 | `AMRX` | no_price | no 09:30 open |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SRFM` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
