# Factor mine action — `union_oppset_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP)

Cash book **-5.36%** ($9,464) · signal-only (no cash/fees) was +17.28%. Starts YES **26/30**. Fills 186 · skips 86 · realized $+810.42.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol).
- Must-have: Theme Radar Clock-B opportunity-set: T−1 gap or RelVol (or week move) flagged — not today's Gap/RelVol.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol) and keep the top 8.
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
- **Gate** `oppset=True` · **rank** `opp_rvol` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,810.39.

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
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $8,764.91 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 115 | $10.83 | $2.33 | — | $7,517.13 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $6,269.36 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `SECZ` | 214 | $5.84 | $2.76 | — | $5,016.84 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-20.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 25 | $48.82 | $2.06 | — | $3,794.27 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `REZI` | 60 | $20.56 | $2.17 | — | $2,558.50 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-21.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `STUB` | 163 | $7.66 | $2.48 | — | $1,307.45 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 50 | $24.68 | $2.14 | — | $71.31 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $71.31 | ▼ close $9,905.62 vs 09:30 $10,000.00 (session -75.93) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.31 | ▼ 09:30 equity $9,794.59 vs yday $9,905.62 (-111.03) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $1,302.02 | ▼ -4.38 after sell → book $9,792.39; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 115 | $11.19 | $2.36 | $+36.70 | $2,586.50 | ▲ +36.70 after sell → book $9,790.02; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $3,655.99 | ▼ -178.28 after sell → book $9,787.67; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SECZ` | 214 | $5.45 | $2.81 | $-89.03 | $4,819.48 | ▼ -89.03 after sell → book $9,784.86; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 25 | $47.39 | $2.08 | $-39.90 | $6,002.15 | ▼ -39.90 after sell → book $9,782.78; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `REZI` | 60 | $20.83 | $2.19 | $+11.84 | $7,249.76 | ▲ +11.84 after sell → book $9,780.59; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `STUB` | 163 | $7.91 | $2.52 | $+35.75 | $8,536.57 | ▲ +35.75 after sell → book $9,778.07; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 50 | $24.83 | $2.16 | $+3.20 | $9,775.91 | ▲ +3.20 after sell → book $9,775.91; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 177 | $6.87 | $2.52 | — | $8,557.40 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+62.6; leftover $1221.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $7,359.65 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+46.0; leftover $1221.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 111 | $10.97 | $2.32 | — | $6,139.66 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $1221.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `RDDT` | 6 | $177.51 | $2.01 | — | $5,072.59 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=+10.1; leftover $1221.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 39 | $31.30 | $2.11 | — | $3,849.78 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-3.8; leftover $1221.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `TSSI` | 127 | $9.61 | $2.37 | — | $2,626.94 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ⚪; ret5=-14.0; leftover $1221.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $1,420.49 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1221.99 | — |
| 2026-08-17 09:30 ET | **BUY** | `NU` | 79 | $15.40 | $2.23 | — | $201.66 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $1221.99 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $201.66 | ▼ close $9,591.07 vs 09:30 $9,794.59 (session -167.10) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $201.66 | ▼ 09:30 equity $9,471.40 vs yday $9,591.07 (-119.67) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $1,403.07 | ▲ +3.66 after sell → book $9,469.31; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 111 | $10.31 | $2.35 | $-77.93 | $2,545.13 | ▼ -77.93 after sell → book $9,466.96; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `RDDT` | 6 | $166.10 | $2.03 | $-72.50 | $3,539.70 | ▼ -72.50 after sell → book $9,464.93; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 39 | $31.31 | $2.13 | $-3.84 | $4,758.66 | ▼ -3.84 after sell → book $9,462.80; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TSSI` | 127 | $9.22 | $2.40 | $-54.30 | $5,927.20 | ▼ -54.30 after sell → book $9,460.40; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $6,982.91 | ▼ -150.74 after sell → book $9,458.28; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NU` | 79 | $14.53 | $2.25 | $-73.21 | $8,128.53 | ▼ -73.21 after sell → book $9,456.03; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,128.53 | ▼ close $9,381.69 vs 09:30 $9,471.40 (session -74.34) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,128.53 | ▲ 09:30 equity $9,401.16 vs yday $9,381.69 (+19.47) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 177 | $7.19 | $2.56 | $+51.56 | $9,398.60 | ▲ +51.56 after sell → book $9,398.60; vs 09:30 mark -2.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,398.60 | ▲ close $9,398.60 vs 09:30 $9,401.16 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,398.60 | ▲ 09:30 equity $9,398.60 vs yday $9,398.60 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,345.61 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1174.82 | — |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 10 | $109.06 | $2.02 | — | $7,252.99 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1174.82 | — |
| 2026-08-20 09:30 ET | **BUY** | `WYFI` | 54 | $21.40 | $2.15 | — | $6,095.23 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-25.2; leftover $1174.82 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 157 | $7.44 | $2.46 | — | $4,924.69 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1174.82 | — |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 34 | $33.61 | $2.09 | — | $3,779.86 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-17.4; leftover $1174.82 | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 12 | $97.43 | $2.03 | — | $2,608.68 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $1174.82 | — |
| 2026-08-20 09:30 ET | **BUY** | `TEM` | 19 | $61.83 | $2.05 | — | $1,431.86 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1174.82 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $273.52 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1174.82 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $273.52 | ▲ close $9,526.08 vs 09:30 $9,398.60 (session +144.31) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $273.52 | ▲ 09:30 equity $9,566.03 vs yday $9,526.08 (+39.95) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 10 | $110.92 | $2.04 | $+14.54 | $1,380.68 | ▲ +14.54 after sell → book $9,563.99; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WYFI` | 54 | $21.54 | $2.17 | $+3.24 | $2,541.67 | ▲ +3.24 after sell → book $9,561.82; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `LZB` | 34 | $33.63 | $2.11 | $-3.52 | $3,682.98 | ▼ -3.52 after sell → book $9,559.71; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 12 | $96.75 | $2.05 | $-12.23 | $4,841.93 | ▼ -12.23 after sell → book $9,557.66; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $6,077.50 | ▲ +77.23 after sell → book $9,555.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AAP` | 28 | $42.41 | $2.07 | — | $4,887.95 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-26.1; leftover $1215.50 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $3,672.46 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1215.50 | — |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 11 | $103.69 | $2.02 | — | $2,529.85 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-10.3; leftover $1215.50 | — |
| 2026-08-21 09:30 ET | **BUY** | `AMRC` | 53 | $22.51 | $2.15 | — | $1,334.67 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-20.2; leftover $1215.50 | — |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 36 | $33.36 | $2.10 | — | $131.61 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1215.50 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.61 | ▲ close $10,021.44 vs 09:30 $9,566.03 (session +476.47) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.61 | ▼ 09:30 equity $9,919.06 vs yday $10,021.44 (-102.38) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,128.48 | ▼ -56.12 after sell → book $9,917.03; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 157 | $8.59 | $2.50 | $+175.59 | $2,474.61 | ▲ +175.59 after sell → book $9,914.54; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 19 | $70.08 | $2.07 | $+152.54 | $3,803.97 | ▲ +152.54 after sell → book $9,912.47; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 28 | $43.05 | $2.09 | $+13.75 | $5,007.27 | ▲ +13.75 after sell → book $9,910.37; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.33 | $2.35 | $+235.14 | $6,457.90 | ▲ +235.14 after sell → book $9,908.03; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 11 | $104.14 | $2.04 | $+0.88 | $7,601.39 | ▲ +0.88 after sell → book $9,905.98; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AMRC` | 53 | $21.19 | $2.17 | $-74.28 | $8,722.29 | ▼ -74.28 after sell → book $9,903.81; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 36 | $32.82 | $2.12 | $-23.66 | $9,901.70 | ▼ -23.66 after sell → book $9,901.70; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,901.70 | ▲ close $9,901.70 vs 09:30 $9,919.06 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,901.70 | ▲ 09:30 equity $9,901.70 vs yday $9,901.70 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 235 | $5.25 | $3.03 | — | $8,664.92 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list mover_buy; 🔵; ⚪; ret5=+10.4; leftover $1237.71 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 194 | $6.37 | $2.57 | — | $7,426.56 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1237.71 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 170 | $7.25 | $2.50 | — | $6,191.56 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1237.71 | — |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 216 | $5.71 | $2.79 | — | $4,955.42 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1237.71 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 236 | $5.24 | $3.04 | — | $3,715.73 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1237.71 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 65 | $19.04 | $2.19 | — | $2,475.95 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+49.5; leftover $1237.71 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 33 | $36.96 | $2.09 | — | $1,254.18 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1237.71 | — |
| 2026-08-25 09:30 ET | **BUY** | `QFIN` | 111 | $11.09 | $2.32 | — | $20.87 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list overnight; 🔵; ret5=-8.0; leftover $1237.71 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.87 | ▲ close $10,319.51 vs 09:30 $9,901.70 (session +438.34) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.87 | ▼ 09:30 equity $9,995.92 vs yday $10,319.51 (-323.59) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ABUS` | 235 | $5.19 | $3.08 | $-20.21 | $1,237.44 | ▼ -20.21 after sell → book $9,992.84; vs 09:30 mark -3.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 194 | $6.13 | $2.61 | $-51.75 | $2,424.04 | ▼ -51.75 after sell → book $9,990.22; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 216 | $5.97 | $2.83 | $+50.54 | $3,710.73 | ▲ +50.54 after sell → book $9,987.39; vs 09:30 mark -2.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 236 | $4.98 | $3.09 | $-67.50 | $4,882.91 | ▼ -67.50 after sell → book $9,984.29; vs 09:30 mark -3.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 65 | $20.72 | $2.21 | $+104.81 | $6,227.51 | ▲ +104.81 after sell → book $9,982.09; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 33 | $38.24 | $2.11 | $+38.04 | $7,487.32 | ▲ +38.04 after sell → book $9,979.98; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 10 | $121.87 | $2.02 | — | $6,266.60 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-35.1; leftover $1247.89 | — |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 74 | $16.77 | $2.21 | — | $5,023.41 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1247.89 | — |
| 2026-08-26 09:30 ET | **BUY** | `MAIR` | 45 | $27.59 | $2.12 | — | $3,779.73 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; ret5=+2.0; leftover $1247.89 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 3 | $326.91 | $2.00 | — | $2,797.00 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-15.2; leftover $1247.89 | — |
| 2026-08-26 09:30 ET | **BUY** | `KURA` | 91 | $13.63 | $2.26 | — | $1,554.41 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $1247.89 | — |
| 2026-08-26 09:30 ET | **BUY** | `SMTC` | 9 | $130.90 | $2.02 | — | $374.29 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-5.7; leftover $1247.89 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $374.29 | ▲ close $10,365.44 vs 09:30 $9,995.92 (session +398.10) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $374.29 | ▲ 09:30 equity $10,403.19 vs yday $10,365.44 (+37.75) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 170 | $9.19 | $2.54 | $+324.76 | $1,934.05 | ▲ +324.76 after sell → book $10,400.65; vs 09:30 mark -2.54 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `QFIN` | 111 | $9.42 | $2.35 | $-190.04 | $2,977.32 | ▼ -190.04 after sell → book $10,398.30; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 10 | $128.73 | $2.04 | $+64.54 | $4,262.58 | ▲ +64.54 after sell → book $10,396.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 74 | $18.50 | $2.24 | $+123.57 | $5,629.35 | ▲ +123.57 after sell → book $10,394.03; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MAIR` | 45 | $28.76 | $2.15 | $+48.38 | $6,921.40 | ▲ +48.38 after sell → book $10,391.88; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 3 | $314.90 | $2.02 | $-40.05 | $7,864.08 | ▼ -40.05 after sell → book $10,389.86; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `KURA` | 91 | $12.98 | $2.29 | $-63.70 | $9,042.97 | ▼ -63.70 after sell → book $10,387.57; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SMTC` | 9 | $149.40 | $2.04 | $+162.45 | $10,385.54 | ▲ +162.45 after sell → book $10,385.54; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,385.54 | ▲ close $10,385.54 vs 09:30 $10,403.19 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,385.54 | ▲ 09:30 equity $10,385.54 vs yday $10,385.54 (-0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $9,214.96 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1298.19 | — |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 71 | $18.15 | $2.20 | — | $7,924.11 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+14.1; leftover $1298.19 | — |
| 2026-08-28 09:30 ET | **BUY** | `QFIN` | 141 | $9.15 | $2.41 | — | $6,631.55 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-19.9; leftover $1298.19 | — |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 81 | $15.88 | $2.23 | — | $5,343.03 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+19.4; leftover $1298.19 | — |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 4 | $306.34 | $2.00 | — | $4,115.67 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-23.0; leftover $1298.19 | — |
| 2026-08-28 09:30 ET | **BUY** | `GENB` | 82 | $15.77 | $2.24 | — | $2,820.29 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-1.4; leftover $1298.19 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $1,547.54 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1298.19 | — |
| 2026-08-28 09:30 ET | **BUY** | `JKS` | 97 | $13.37 | $2.28 | — | $248.37 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-14.9; leftover $1298.19 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $248.37 | ▼ close $10,233.78 vs 09:30 $10,385.54 (session -134.34) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $248.37 | ▼ 09:30 equity $10,212.87 vs yday $10,233.78 (-20.91) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $1,430.57 | ▲ +11.63 after sell → book $10,210.83; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 71 | $17.70 | $2.22 | $-36.38 | $2,685.05 | ▼ -36.38 after sell → book $10,208.61; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 141 | $8.70 | $2.45 | $-68.31 | $3,909.30 | ▼ -68.31 after sell → book $10,206.16; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 81 | $15.46 | $2.26 | $-38.51 | $5,159.30 | ▼ -38.51 after sell → book $10,203.90; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 4 | $298.01 | $2.02 | $-37.34 | $6,349.32 | ▼ -37.34 after sell → book $10,201.88; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GENB` | 82 | $15.27 | $2.26 | $-45.50 | $7,599.20 | ▼ -45.50 after sell → book $10,199.62; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $8,884.18 | ▲ +12.22 after sell → book $10,197.56; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `JKS` | 97 | $13.54 | $2.31 | $+11.90 | $10,195.26 | ▲ +11.90 after sell → book $10,195.26; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,195.26 | ▲ close $10,195.26 vs 09:30 $10,212.87 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,195.26 | ▲ 09:30 equity $10,195.26 vs yday $10,195.26 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,195.26 | ▲ close $10,195.26 vs 09:30 $10,195.26 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,195.26 | ▲ 09:30 equity $10,195.26 vs yday $10,195.26 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,195.26 | ▲ close $10,195.26 vs 09:30 $10,195.26 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,195.26 | ▲ 09:30 equity $10,195.26 vs yday $10,195.26 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 122 | $10.38 | $2.36 | — | $8,927.15 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-56.2; leftover $1274.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 69 | $18.28 | $2.20 | — | $7,663.63 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+16.5; leftover $1274.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $6,689.02 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list mover_buy; 🔵; ret5=+6.1; leftover $1274.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 92 | $13.71 | $2.27 | — | $5,425.43 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+17.5; leftover $1274.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $55.42 | $2.06 | — | $4,204.13 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-25.9; leftover $1274.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $23.88 | $2.15 | — | $2,936.35 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1274.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 190 | $6.68 | $2.56 | — | $1,664.59 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+11.4; leftover $1274.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 9 | $127.91 | $2.02 | — | $511.38 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1274.41 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $511.38 | ▲ close $10,338.60 vs 09:30 $10,195.26 (session +160.94) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $511.38 | ▼ 09:30 equity $10,295.99 vs yday $10,338.60 (-42.61) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 122 | $11.23 | $2.39 | $+99.57 | $1,879.05 | ▲ +99.57 after sell → book $10,293.60; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 69 | $17.27 | $2.22 | $-74.11 | $3,068.46 | ▼ -74.11 after sell → book $10,291.38; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 92 | $13.89 | $2.29 | $+12.00 | $4,344.05 | ▲ +12.00 after sell → book $10,289.09; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.79 | $2.08 | $+4.01 | $5,569.36 | ▲ +4.01 after sell → book $10,287.02; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 53 | $23.84 | $2.17 | $-6.44 | $6,830.71 | ▼ -6.44 after sell → book $10,284.85; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 9 | $125.22 | $2.04 | $-28.26 | $7,955.65 | ▼ -28.26 after sell → book $10,282.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 13 | $98.15 | $2.03 | — | $6,677.67 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=+5.9; leftover $1325.94 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 52 | $25.18 | $2.15 | — | $5,366.16 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+16.0; leftover $1325.94 | — |
| 2026-09-04 09:30 ET | **BUY** | `PL` | 67 | $19.64 | $2.19 | — | $4,048.09 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-13.3; leftover $1325.94 | — |
| 2026-09-04 09:30 ET | **BUY** | `ZS` | 7 | $166.15 | $2.01 | — | $2,883.03 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-5.1; leftover $1325.94 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 29 | $44.90 | $2.08 | — | $1,578.86 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-7.5; leftover $1325.94 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 17 | $75.65 | $2.04 | — | $290.76 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1325.94 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $290.76 | ▼ close $10,194.18 vs 09:30 $10,295.99 (session -76.13) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $290.76 | ▼ 09:30 equity $10,085.66 vs yday $10,194.18 (-108.52) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+65.67 | $1,331.05 | ▲ +65.67 after sell → book $10,083.64; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `RSKD` | 190 | $6.46 | $2.60 | $-46.96 | $2,555.85 | ▼ -46.96 after sell → book $10,081.04; vs 09:30 mark -2.60 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 13 | $100.58 | $2.05 | $+27.51 | $3,861.34 | ▲ +27.51 after sell → book $10,078.99; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 52 | $26.44 | $2.17 | $+61.21 | $5,234.05 | ▲ +61.21 after sell → book $10,076.83; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `PL` | 67 | $17.85 | $2.21 | $-124.33 | $6,427.79 | ▼ -124.33 after sell → book $10,074.61; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZS` | 7 | $165.62 | $2.03 | $-7.79 | $7,585.06 | ▼ -7.79 after sell → book $10,072.58; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 29 | $39.56 | $2.10 | $-159.03 | $8,730.21 | ▼ -159.03 after sell → book $10,070.49; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 17 | $78.84 | $2.06 | $+50.13 | $10,068.42 | ▲ +50.13 after sell → book $10,068.42; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,068.42 | ▲ close $10,068.42 vs 09:30 $10,085.66 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,068.42 | ▲ 09:30 equity $10,068.42 vs yday $10,068.42 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,068.42 | ▲ close $10,068.42 vs 09:30 $10,068.42 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,068.42 | ▲ 09:30 equity $10,068.42 vs yday $10,068.42 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,068.42 | ▲ close $10,068.42 vs 09:30 $10,068.42 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,068.42 | ▲ 09:30 equity $10,068.42 vs yday $10,068.42 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `BKV` | 50 | $24.97 | $2.14 | — | $8,817.78 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; ret5=+10.8; leftover $1258.55 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 23 | $54.66 | $2.06 | — | $7,558.54 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-22.3; leftover $1258.55 | — |
| 2026-09-11 09:30 ET | **BUY** | `AEO` | 85 | $14.71 | $2.25 | — | $6,305.95 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-12.8; leftover $1258.55 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 114 | $10.95 | $2.33 | — | $5,055.32 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1258.55 | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 61 | $20.61 | $2.17 | — | $3,795.93 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-24.7; leftover $1258.55 | — |
| 2026-09-11 09:30 ET | **BUY** | `TSSI` | 140 | $8.98 | $2.41 | — | $2,536.32 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+14.1; leftover $1258.55 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 204 | $6.16 | $2.63 | — | $1,277.05 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+36.4; leftover $1258.55 | — |
| 2026-09-11 09:30 ET | **BUY** | `AXGN` | 29 | $42.48 | $2.08 | — | $43.06 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-13.4; leftover $1258.55 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.06 | ▼ close $9,941.73 vs 09:30 $10,068.42 (session -108.63) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.06 | ▼ 09:30 equity $9,871.24 vs yday $9,941.73 (-70.49) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 23 | $54.78 | $2.08 | $-1.38 | $1,300.92 | ▼ -1.38 after sell → book $9,869.16; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AEO` | 85 | $14.85 | $2.27 | $+7.39 | $2,560.90 | ▲ +7.39 after sell → book $9,866.89; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 114 | $10.29 | $2.36 | $-79.93 | $3,731.60 | ▼ -79.93 after sell → book $9,864.53; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `NAVN` | 61 | $21.10 | $2.19 | $+25.52 | $5,016.50 | ▲ +25.52 after sell → book $9,862.33; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TSSI` | 140 | $8.57 | $2.44 | $-62.25 | $6,213.86 | ▼ -62.25 after sell → book $9,859.89; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 204 | $6.02 | $2.68 | $-33.87 | $7,439.26 | ▼ -33.87 after sell → book $9,857.21; vs 09:30 mark -2.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AXGN` | 29 | $41.55 | $2.10 | $-31.14 | $8,642.12 | ▼ -31.14 after sell → book $9,855.12; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,642.12 | ▼ close $9,833.12 vs 09:30 $9,871.24 (session -22.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,642.12 | ▲ 09:30 equity $9,854.62 vs yday $9,833.12 (+21.50) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BKV` | 50 | $24.25 | $2.16 | $-40.30 | $9,852.46 | ▼ -40.30 after sell → book $9,852.46; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,852.46 | ▲ close $9,852.46 vs 09:30 $9,854.62 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,852.46 | ▲ 09:30 equity $9,852.46 vs yday $9,852.46 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 179 | $6.86 | $2.53 | — | $8,621.99 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-22.4; leftover $1231.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 119 | $10.30 | $2.35 | — | $7,393.94 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-23.0; leftover $1231.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 37 | $33.14 | $2.10 | — | $6,165.66 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=-2.9; leftover $1231.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `GFR` | 180 | $6.83 | $2.53 | — | $4,933.73 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; ret5=+11.2; leftover $1231.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `HQ` | 95 | $12.89 | $2.27 | — | $3,706.91 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=-18.2; leftover $1231.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 52 | $23.29 | $2.15 | — | $2,493.68 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+16.1; leftover $1231.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `DMRA` | 49 | $24.88 | $2.14 | — | $1,272.42 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-15.2; leftover $1231.56 | — |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 8 | $140.88 | $2.01 | — | $143.37 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1231.56 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.37 | ▼ close $9,765.45 vs 09:30 $9,852.46 (session -68.93) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.37 | ▲ 09:30 equity $9,881.55 vs yday $9,765.45 (+116.10) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `PLAY` | 179 | $6.96 | $2.57 | $+12.81 | $1,386.64 | ▲ +12.81 after sell → book $9,878.98; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `GFR` | 180 | $6.48 | $2.57 | $-68.10 | $2,550.47 | ▼ -68.10 after sell → book $9,876.41; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `HQ` | 95 | $13.56 | $2.30 | $+59.07 | $3,836.37 | ▲ +59.07 after sell → book $9,874.11; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 52 | $24.09 | $2.17 | $+37.29 | $5,086.89 | ▲ +37.29 after sell → book $9,871.95; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `DMRA` | 49 | $24.96 | $2.16 | $-0.37 | $6,307.77 | ▼ -0.37 after sell → book $9,869.79; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RVTY` | 8 | $147.61 | $2.03 | $+49.79 | $7,486.62 | ▲ +49.79 after sell → book $9,867.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 55 | $22.46 | $2.15 | — | $6,249.16 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $1247.77 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $5,054.16 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-11.6; leftover $1247.77 | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 111 | $11.21 | $2.32 | — | $3,807.52 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=+1.0; leftover $1247.77 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 48 | $25.95 | $2.13 | — | $2,559.79 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1247.77 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMRX` | 67 | $18.56 | $2.19 | — | $1,314.08 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+4.8; leftover $1247.77 | — |
| 2026-09-17 09:30 ET | **BUY** | `BTGO` | 190 | $6.56 | $2.56 | — | $65.12 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-14.6; leftover $1247.77 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.12 | ▲ close $9,932.02 vs 09:30 $9,881.55 (session +77.63) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.12 | ▲ 09:30 equity $9,994.44 vs yday $9,932.02 (+62.42) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ALHC` | 119 | $8.68 | $2.38 | $-197.50 | $1,095.66 | ▼ -197.50 after sell → book $9,992.06; vs 09:30 mark -2.38 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 37 | $39.50 | $2.12 | $+231.10 | $2,555.04 | ▲ +231.10 after sell → book $9,989.94; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 55 | $21.30 | $2.17 | $-68.13 | $3,724.36 | ▼ -68.13 after sell → book $9,987.76; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $4,906.34 | ▼ -13.03 after sell → book $9,985.74; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMRX` | 67 | $18.12 | $2.21 | $-33.88 | $6,118.17 | ▼ -33.88 after sell → book $9,983.53; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BTGO` | 190 | $6.94 | $2.60 | $+67.04 | $7,434.16 | ▲ +67.04 after sell → book $9,980.92; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $6,384.56 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1239.03 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 42 | $29.32 | $2.12 | — | $5,151.00 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1239.03 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 164 | $7.54 | $2.48 | — | $3,912.78 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-20.9; leftover $1239.03 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 83 | $14.79 | $2.24 | — | $2,682.97 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1239.03 | — |
| 2026-09-18 09:30 ET | **BUY** | `SECZ` | 132 | $9.32 | $2.39 | — | $1,450.35 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+11.1; leftover $1239.03 | — |
| 2026-09-18 09:30 ET | **BUY** | `USDE` | 129 | $9.54 | $2.38 | — | $217.31 | — | Theme Radar Clock-B oppset ∩ union, rank T−1 rvol (research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+15.8; leftover $1239.03 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $217.31 | ▲ close $10,256.90 vs 09:30 $9,994.44 (session +289.58) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $217.31 | ▲ 09:30 equity $10,828.67 vs yday $10,256.90 (+571.77) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `ALMU` | 111 | $13.12 | $2.35 | $+207.89 | $1,671.83 | ▲ +207.89 after sell → book $10,826.32; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQT` | 48 | $25.57 | $2.15 | $-22.53 | $2,897.04 | ▼ -22.53 after sell → book $10,824.17; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 5 | $210.00 | $2.02 | $-1.63 | $3,945.01 | ▼ -1.63 after sell → book $10,822.14; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 42 | $29.43 | $2.14 | $+0.37 | $5,178.94 | ▲ +0.37 after sell → book $10,820.01; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 164 | $7.36 | $2.52 | $-33.70 | $6,383.46 | ▼ -33.70 after sell → book $10,817.49; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 83 | $14.58 | $2.26 | $-21.93 | $7,591.33 | ▼ -21.93 after sell → book $10,815.22; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SECZ` | 132 | $11.67 | $2.42 | $+305.39 | $9,129.35 | ▲ +305.39 after sell → book $10,812.80; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `USDE` | 129 | $13.05 | $2.41 | $+448.00 | $10,810.39 | ▲ +448.00 after sell → book $10,810.39; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,810.39 | ▲ close $10,810.39 vs 09:30 $10,828.67 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,810.39 | ▲ 09:30 equity $10,810.39 vs yday $10,810.39 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,810.39 | ▲ close $10,810.39 vs 09:30 $10,810.39 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,810.39 | ▲ 09:30 equity $10,810.39 vs yday $10,810.39 (+0.00) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,810.39 | ▲ close $10,810.39 vs 09:30 $10,810.39 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,810.39 | ▲ 09:30 equity $10,810.39 vs yday $10,810.39 (+0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,810.39 | ▲ close $10,810.39 vs 09:30 $10,810.39 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,463.98 | ▲ 09:30 equity $9,463.98 vs yday $9,463.98 (+0.00) | 09:30 open · cash $9,463.98 · no holdings · equity $9,463.98 vs prior close $9,463.98 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,463.98 | ▲ close $9,463.98 vs 09:30 $9,463.98 (session +0.00) | 16:00 close · cash $9,463.98 · no lots left · equity $9,463.98. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `FN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `VNET` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PAAS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `METC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `HAL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TSLA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ENOV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CHA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `IONS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SGML` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TYRA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EVMN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NMAX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `QRVO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `RUM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
