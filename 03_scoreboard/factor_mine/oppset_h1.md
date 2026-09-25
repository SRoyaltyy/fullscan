# Factor mine action — `oppset_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `oppset` · top 8 · rank `opp_rvol` · size `leftover` · sell `list` · S-boost `none` · Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP)

Cash book **-4.53%** ($9,548) · signal-only (no cash/fees) was +17.09%. Starts YES **26/30**. Fills 186 · skips 85 · realized $+634.68.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at Theme Radar Clock-B opportunity-set (T−1 gap + RelVol flagged; optional feed) and only buy names that pass every must-have on the checklist. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: Theme Radar Clock-B opportunity-set (T−1 gap + RelVol flagged; optional feed).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: Theme Radar T−1 relative volume (Clock-B opportunity-set; not same-day RelVol).
- Must-have: Theme Radar Clock-B opportunity-set: T−1 gap or RelVol (or week move) flagged — not today's Gap/RelVol.

### When it buys

- At 09:30, take names on Theme Radar Clock-B opportunity-set (T−1 gap + RelVol flagged; optional feed) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
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

- **Universe** `oppset` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `oppset=True` · **rank** `opp_rvol` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,634.66.

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
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $8,764.91 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 115 | $10.83 | $2.33 | — | $7,517.13 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `OMER` | 72 | $17.35 | $2.21 | — | $6,265.72 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $5,017.95 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `SECZ` | 214 | $5.84 | $2.76 | — | $3,765.43 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ⚪; ret5=-20.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AVAH` | 104 | $11.91 | $2.30 | — | $2,524.49 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+21.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `CRMD` | 155 | $8.05 | $2.46 | — | $1,274.29 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 25 | $48.82 | $2.06 | — | $51.72 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.72 | ▼ close $9,721.15 vs 09:30 $10,000.00 (session -260.22) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.72 | ▼ 09:30 equity $9,670.70 vs yday $9,721.15 (-50.45) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $1,282.43 | ▼ -4.38 after sell → book $9,668.50; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 115 | $11.19 | $2.36 | $+36.70 | $2,566.92 | ▲ +36.70 after sell → book $9,666.14; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `OMER` | 72 | $17.17 | $2.23 | $-17.39 | $3,800.93 | ▼ -17.39 after sell → book $9,663.91; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $4,870.41 | ▼ -178.28 after sell → book $9,661.55; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SECZ` | 214 | $5.45 | $2.81 | $-89.03 | $6,033.91 | ▼ -89.03 after sell → book $9,658.75; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AVAH` | 104 | $12.21 | $2.33 | $+26.57 | $7,301.42 | ▲ +26.57 after sell → book $9,656.42; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CRMD` | 155 | $7.55 | $2.49 | $-82.45 | $8,469.18 | ▼ -82.45 after sell → book $9,653.93; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 25 | $47.39 | $2.08 | $-39.90 | $9,651.84 | ▼ -39.90 after sell → book $9,651.84; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 175 | $6.87 | $2.52 | — | $8,447.08 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+62.6; leftover $1206.48 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $7,249.33 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+46.0; leftover $1206.48 | — |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 109 | $10.97 | $2.32 | — | $6,051.28 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $1206.48 | — |
| 2026-08-17 09:30 ET | **BUY** | `RDDT` | 6 | $177.51 | $2.01 | — | $4,984.22 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=+10.1; leftover $1206.48 | — |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 38 | $31.30 | $2.10 | — | $3,792.71 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-3.8; leftover $1206.48 | — |
| 2026-08-17 09:30 ET | **BUY** | `TSSI` | 125 | $9.61 | $2.37 | — | $2,589.10 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ⚪; ret5=-14.0; leftover $1206.48 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $1,382.65 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1206.48 | — |
| 2026-08-17 09:30 ET | **BUY** | `BYND` | 94 | $12.83 | $2.27 | — | $174.35 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $1206.48 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.35 | ▼ close $9,406.31 vs 09:30 $9,670.70 (session -227.77) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.35 | ▼ 09:30 equity $9,256.13 vs yday $9,406.31 (-150.18) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $1,375.76 | ▲ +3.66 after sell → book $9,254.04; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 109 | $10.31 | $2.35 | $-76.60 | $2,497.20 | ▼ -76.60 after sell → book $9,251.69; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `RDDT` | 6 | $166.10 | $2.03 | $-72.50 | $3,491.77 | ▼ -72.50 after sell → book $9,249.66; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 38 | $31.31 | $2.12 | $-3.85 | $4,679.43 | ▼ -3.85 after sell → book $9,247.54; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TSSI` | 125 | $9.22 | $2.40 | $-53.51 | $5,829.53 | ▼ -53.51 after sell → book $9,245.14; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $6,885.24 | ▼ -150.74 after sell → book $9,243.02; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BYND` | 94 | $11.12 | $2.30 | $-165.31 | $7,928.23 | ▼ -165.31 after sell → book $9,240.73; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,928.23 | ▼ close $9,167.23 vs 09:30 $9,256.13 (session -73.50) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,928.23 | ▲ 09:30 equity $9,186.48 vs yday $9,167.23 (+19.25) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 175 | $7.19 | $2.55 | $+50.93 | $9,183.92 | ▲ +50.93 after sell → book $9,183.92; vs 09:30 mark -2.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,183.92 | ▲ close $9,183.92 vs 09:30 $9,186.48 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,183.92 | ▲ 09:30 equity $9,183.92 vs yday $9,183.92 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,130.93 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1147.99 | — |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 10 | $109.06 | $2.02 | — | $7,038.31 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1147.99 | — |
| 2026-08-20 09:30 ET | **BUY** | `WYFI` | 53 | $21.40 | $2.15 | — | $5,901.96 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-25.2; leftover $1147.99 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 154 | $7.44 | $2.45 | — | $4,753.75 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1147.99 | — |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 34 | $33.61 | $2.09 | — | $3,608.92 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-17.4; leftover $1147.99 | — |
| 2026-08-20 09:30 ET | **BUY** | `EL` | 11 | $97.43 | $2.02 | — | $2,535.17 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+11.8; leftover $1147.99 | — |
| 2026-08-20 09:30 ET | **BUY** | `TEM` | 18 | $61.83 | $2.04 | — | $1,420.18 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1147.99 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $406.39 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1147.99 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $406.39 | ▲ close $9,299.87 vs 09:30 $9,183.92 (session +132.75) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $406.39 | ▲ 09:30 equity $9,335.47 vs yday $9,299.87 (+35.60) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 10 | $110.92 | $2.04 | $+14.54 | $1,513.55 | ▲ +14.54 after sell → book $9,333.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WYFI` | 53 | $21.54 | $2.17 | $+3.10 | $2,653.00 | ▲ +3.10 after sell → book $9,331.26; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `LZB` | 34 | $33.63 | $2.11 | $-3.52 | $3,794.31 | ▼ -3.52 after sell → book $9,329.15; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EL` | 11 | $96.75 | $2.04 | $-11.55 | $4,856.52 | ▼ -11.55 after sell → book $9,327.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 7 | $154.70 | $2.03 | $+67.08 | $5,937.39 | ▲ +67.08 after sell → book $9,325.08; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AAP` | 27 | $42.41 | $2.07 | — | $4,790.24 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-26.1; leftover $1187.48 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 106 | $11.13 | $2.31 | — | $3,608.16 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1187.48 | — |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 11 | $103.69 | $2.02 | — | $2,465.54 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-10.3; leftover $1187.48 | — |
| 2026-08-21 09:30 ET | **BUY** | `AMRC` | 52 | $22.51 | $2.15 | — | $1,292.88 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-20.2; leftover $1187.48 | — |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 35 | $33.36 | $2.10 | — | $123.18 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1187.48 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.18 | ▲ close $9,776.64 vs 09:30 $9,335.47 (session +462.21) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.18 | ▼ 09:30 equity $9,677.74 vs yday $9,776.64 (-98.90) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,120.05 | ▼ -56.12 after sell → book $9,675.71; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 154 | $8.59 | $2.49 | $+172.16 | $2,440.42 | ▲ +172.16 after sell → book $9,673.22; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 18 | $70.08 | $2.06 | $+144.30 | $3,699.71 | ▲ +144.30 after sell → book $9,671.16; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 27 | $43.05 | $2.09 | $+13.12 | $4,859.97 | ▲ +13.12 after sell → book $9,669.07; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 11 | $104.14 | $2.04 | $+0.88 | $6,003.46 | ▲ +0.88 after sell → book $9,667.02; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AMRC` | 52 | $21.19 | $2.17 | $-72.95 | $7,103.18 | ▼ -72.95 after sell → book $9,664.86; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 35 | $32.82 | $2.12 | $-23.11 | $8,249.76 | ▼ -23.11 after sell → book $9,662.74; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,249.76 | ▲ close $9,769.80 vs 09:30 $9,677.74 (session +107.06) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,249.76 | ▼ 09:30 equity $9,746.48 vs yday $9,769.80 (-23.32) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `ARCT` | 106 | $14.12 | $2.34 | $+312.29 | $9,744.15 | ▲ +312.29 after sell → book $9,744.15; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `ABUS` | 232 | $5.25 | $2.99 | — | $8,523.15 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list mover_buy; 🔵; ⚪; ret5=+10.4; leftover $1218.02 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 191 | $6.37 | $2.56 | — | $7,303.92 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1218.02 | — |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 168 | $7.25 | $2.49 | — | $6,083.43 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=-8.7; leftover $1218.02 | — |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 213 | $5.71 | $2.75 | — | $4,864.45 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1218.02 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 232 | $5.24 | $2.99 | — | $3,645.78 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1218.02 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 63 | $19.04 | $2.18 | — | $2,444.08 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+49.5; leftover $1218.02 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 32 | $36.96 | $2.09 | — | $1,259.27 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1218.02 | — |
| 2026-08-25 09:30 ET | **BUY** | `QFIN` | 109 | $11.09 | $2.32 | — | $48.14 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list overnight; 🔵; ret5=-8.0; leftover $1218.02 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.14 | ▲ close $10,152.89 vs 09:30 $9,746.48 (session +429.12) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.14 | ▼ 09:30 equity $9,835.62 vs yday $10,152.89 (-317.27) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ABUS` | 232 | $5.19 | $3.04 | $-19.95 | $1,249.18 | ▼ -19.95 after sell → book $9,832.58; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 191 | $6.13 | $2.60 | $-51.01 | $2,417.41 | ▼ -51.01 after sell → book $9,829.98; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 213 | $5.97 | $2.79 | $+49.84 | $3,686.22 | ▲ +49.84 after sell → book $9,827.18; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 232 | $4.98 | $3.04 | $-66.35 | $4,838.54 | ▼ -66.35 after sell → book $9,824.14; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 63 | $20.72 | $2.20 | $+101.46 | $6,141.70 | ▲ +101.46 after sell → book $9,821.94; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `LIFE` | 32 | $38.24 | $2.11 | $+36.77 | $7,363.28 | ▲ +36.77 after sell → book $9,819.84; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 10 | $121.87 | $2.02 | — | $6,142.56 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-35.1; leftover $1227.21 | — |
| 2026-08-26 09:30 ET | **BUY** | `BZ` | 73 | $16.77 | $2.21 | — | $4,916.14 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+3.1; leftover $1227.21 | — |
| 2026-08-26 09:30 ET | **BUY** | `MAIR` | 44 | $27.59 | $2.12 | — | $3,700.06 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; ret5=+2.0; leftover $1227.21 | — |
| 2026-08-26 09:30 ET | **BUY** | `DY` | 3 | $326.91 | $2.00 | — | $2,717.33 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-15.2; leftover $1227.21 | — |
| 2026-08-26 09:30 ET | **BUY** | `KURA` | 90 | $13.63 | $2.26 | — | $1,488.37 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+10.2; leftover $1227.21 | — |
| 2026-08-26 09:30 ET | **BUY** | `SMTC` | 9 | $130.90 | $2.02 | — | $308.25 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; 🔵; ret5=-5.7; leftover $1227.21 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $308.25 | ▲ close $10,201.57 vs 09:30 $9,835.62 (session +394.36) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $308.25 | ▲ 09:30 equity $10,239.69 vs yday $10,201.57 (+38.12) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 168 | $9.19 | $2.53 | $+320.89 | $1,849.64 | ▲ +320.89 after sell → book $10,237.16; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `QFIN` | 109 | $9.42 | $2.35 | $-186.69 | $2,874.07 | ▼ -186.69 after sell → book $10,234.81; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 10 | $128.73 | $2.04 | $+64.54 | $4,159.33 | ▲ +64.54 after sell → book $10,232.77; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 73 | $18.50 | $2.23 | $+121.85 | $5,507.60 | ▲ +121.85 after sell → book $10,230.54; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MAIR` | 44 | $28.76 | $2.14 | $+47.22 | $6,770.90 | ▲ +47.22 after sell → book $10,228.40; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DY` | 3 | $314.90 | $2.02 | $-40.05 | $7,713.58 | ▼ -40.05 after sell → book $10,226.38; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `KURA` | 90 | $12.98 | $2.28 | $-63.04 | $8,879.49 | ▼ -63.04 after sell → book $10,224.09; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SMTC` | 9 | $149.40 | $2.04 | $+162.45 | $10,222.06 | ▲ +162.45 after sell → book $10,222.06; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,222.06 | ▲ close $10,222.06 vs 09:30 $10,239.69 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,222.06 | ▲ 09:30 equity $10,222.06 vs yday $10,222.06 (-0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $146.07 | $2.01 | — | $9,051.48 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1277.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 70 | $18.15 | $2.20 | — | $7,778.78 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+14.1; leftover $1277.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `QFIN` | 139 | $9.15 | $2.41 | — | $6,504.52 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-19.9; leftover $1277.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 80 | $15.88 | $2.23 | — | $5,231.89 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+19.4; leftover $1277.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 4 | $306.34 | $2.00 | — | $4,004.53 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-23.0; leftover $1277.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `GENB` | 81 | $15.77 | $2.23 | — | $2,724.93 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-1.4; leftover $1277.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $79.42 | $2.04 | — | $1,452.17 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.5; leftover $1277.76 | — |
| 2026-08-28 09:30 ET | **BUY** | `JKS` | 95 | $13.37 | $2.27 | — | $179.75 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-14.9; leftover $1277.76 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.75 | ▼ close $10,071.94 vs 09:30 $10,222.06 (session -132.72) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.75 | ▼ 09:30 equity $10,051.34 vs yday $10,071.94 (-20.60) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.03 | $2.03 | $+11.63 | $1,361.95 | ▲ +11.63 after sell → book $10,049.30; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 70 | $17.70 | $2.22 | $-35.92 | $2,598.73 | ▼ -35.92 after sell → book $10,047.08; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 139 | $8.70 | $2.44 | $-67.40 | $3,805.59 | ▼ -67.40 after sell → book $10,044.64; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 80 | $15.46 | $2.25 | $-38.08 | $5,040.14 | ▼ -38.08 after sell → book $10,042.39; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 4 | $298.01 | $2.02 | $-37.34 | $6,230.16 | ▼ -37.34 after sell → book $10,040.37; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GENB` | 81 | $15.27 | $2.26 | $-44.99 | $7,464.77 | ▼ -44.99 after sell → book $10,038.11; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $80.44 | $2.06 | $+12.22 | $8,749.75 | ▲ +12.22 after sell → book $10,036.05; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `JKS` | 95 | $13.54 | $2.30 | $+11.57 | $10,033.75 | ▲ +11.57 after sell → book $10,033.75; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,033.75 | ▲ close $10,033.75 vs 09:30 $10,051.34 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,033.75 | ▲ 09:30 equity $10,033.75 vs yday $10,033.75 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,033.75 | ▲ close $10,033.75 vs 09:30 $10,033.75 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,033.75 | ▲ 09:30 equity $10,033.75 vs yday $10,033.75 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,033.75 | ▲ close $10,033.75 vs 09:30 $10,033.75 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,033.75 | ▲ 09:30 equity $10,033.75 vs yday $10,033.75 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 120 | $10.38 | $2.35 | — | $8,786.40 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-56.2; leftover $1254.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 68 | $18.28 | $2.19 | — | $7,541.17 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+16.5; leftover $1254.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $6,566.55 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list mover_buy; 🔵; ret5=+6.1; leftover $1254.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 91 | $13.71 | $2.26 | — | $5,316.68 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+17.5; leftover $1254.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $55.42 | $2.06 | — | $4,095.38 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; ret5=-25.9; leftover $1254.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $23.88 | $2.15 | — | $2,851.47 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1254.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `RSKD` | 187 | $6.68 | $2.55 | — | $1,599.76 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+11.4; leftover $1254.22 | — |
| 2026-09-03 09:30 ET | **BUY** | `AGCO` | 9 | $127.91 | $2.02 | — | $446.56 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.6; leftover $1254.22 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $446.56 | ▲ close $10,175.43 vs 09:30 $10,033.75 (session +159.25) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $446.56 | ▼ 09:30 equity $10,133.19 vs yday $10,175.43 (-42.24) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 120 | $11.23 | $2.38 | $+97.87 | $1,791.78 | ▲ +97.87 after sell → book $10,130.81; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 68 | $17.27 | $2.22 | $-73.09 | $2,963.92 | ▼ -73.09 after sell → book $10,128.59; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 91 | $13.89 | $2.29 | $+11.83 | $4,225.62 | ▲ +11.83 after sell → book $10,126.30; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.79 | $2.08 | $+4.01 | $5,450.93 | ▲ +4.01 after sell → book $10,124.23; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 52 | $23.84 | $2.17 | $-6.39 | $6,688.44 | ▼ -6.39 after sell → book $10,122.06; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AGCO` | 9 | $125.22 | $2.04 | $-28.26 | $7,813.38 | ▼ -28.26 after sell → book $10,120.02; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 13 | $98.15 | $2.03 | — | $6,535.40 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=+5.9; leftover $1302.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 51 | $25.18 | $2.14 | — | $5,249.08 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ret5=+16.0; leftover $1302.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `PL` | 66 | $19.64 | $2.19 | — | $3,950.65 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-13.3; leftover $1302.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `ZS` | 7 | $166.15 | $2.01 | — | $2,785.59 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-5.1; leftover $1302.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 29 | $44.90 | $2.08 | — | $1,481.42 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=-7.5; leftover $1302.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 17 | $75.65 | $2.04 | — | $193.32 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1302.23 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.32 | ▼ close $10,031.95 vs 09:30 $10,133.19 (session -75.58) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.32 | ▼ 09:30 equity $9,924.55 vs yday $10,031.95 (-107.40) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+65.67 | $1,233.61 | ▲ +65.67 after sell → book $9,922.53; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `RSKD` | 187 | $6.46 | $2.59 | $-46.28 | $2,439.04 | ▼ -46.28 after sell → book $9,919.94; vs 09:30 mark -2.59 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 13 | $100.58 | $2.05 | $+27.51 | $3,744.53 | ▲ +27.51 after sell → book $9,917.89; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 51 | $26.44 | $2.16 | $+59.95 | $5,090.80 | ▲ +59.95 after sell → book $9,915.73; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `PL` | 66 | $17.85 | $2.21 | $-122.54 | $6,266.69 | ▼ -122.54 after sell → book $9,913.52; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ZS` | 7 | $165.62 | $2.03 | $-7.79 | $7,423.97 | ▼ -7.79 after sell → book $9,911.49; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 29 | $39.56 | $2.10 | $-159.03 | $8,569.11 | ▼ -159.03 after sell → book $9,909.39; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 17 | $78.84 | $2.06 | $+50.13 | $9,907.33 | ▲ +50.13 after sell → book $9,907.33; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,907.33 | ▲ close $9,907.33 vs 09:30 $9,924.55 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,907.33 | ▲ 09:30 equity $9,907.33 vs yday $9,907.33 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,907.33 | ▲ close $9,907.33 vs 09:30 $9,907.33 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,907.33 | ▲ 09:30 equity $9,907.33 vs yday $9,907.33 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,907.33 | ▲ close $9,907.33 vs 09:30 $9,907.33 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,907.33 | ▲ 09:30 equity $9,907.33 vs yday $9,907.33 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `BKV` | 49 | $24.97 | $2.14 | — | $8,681.66 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; ret5=+10.8; leftover $1238.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 22 | $54.66 | $2.06 | — | $7,477.09 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-22.3; leftover $1238.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `AEO` | 84 | $14.71 | $2.24 | — | $6,239.20 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-12.8; leftover $1238.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 113 | $10.95 | $2.33 | — | $4,999.53 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1238.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 60 | $20.61 | $2.17 | — | $3,760.76 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-24.7; leftover $1238.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `TSSI` | 137 | $8.98 | $2.40 | — | $2,528.09 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+14.1; leftover $1238.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 201 | $6.16 | $2.60 | — | $1,287.34 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+36.4; leftover $1238.42 | — |
| 2026-09-11 09:30 ET | **BUY** | `AXGN` | 29 | $42.48 | $2.08 | — | $53.34 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-13.4; leftover $1238.42 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.34 | ▼ close $9,782.54 vs 09:30 $9,907.33 (session -106.78) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.34 | ▼ 09:30 equity $9,712.47 vs yday $9,782.54 (-70.07) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 22 | $54.78 | $2.08 | $-1.49 | $1,256.42 | ▼ -1.49 after sell → book $9,710.39; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AEO` | 84 | $14.85 | $2.27 | $+7.25 | $2,501.56 | ▲ +7.25 after sell → book $9,708.13; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 113 | $10.29 | $2.36 | $-79.27 | $3,661.97 | ▼ -79.27 after sell → book $9,705.77; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `NAVN` | 60 | $21.10 | $2.19 | $+25.04 | $4,925.78 | ▲ +25.04 after sell → book $9,703.58; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `TSSI` | 137 | $8.57 | $2.43 | $-61.00 | $6,097.44 | ▼ -61.00 after sell → book $9,701.15; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 201 | $6.02 | $2.64 | $-33.38 | $7,304.81 | ▼ -33.38 after sell → book $9,698.50; vs 09:30 mark -2.65 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AXGN` | 29 | $41.55 | $2.10 | $-31.14 | $8,507.67 | ▼ -31.14 after sell → book $9,696.41; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,507.67 | ▼ close $9,674.85 vs 09:30 $9,712.47 (session -21.56) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,507.67 | ▲ 09:30 equity $9,695.92 vs yday $9,674.85 (+21.07) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BKV` | 49 | $24.25 | $2.16 | $-39.57 | $9,693.76 | ▼ -39.57 after sell → book $9,693.76; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,693.76 | ▲ close $9,693.76 vs 09:30 $9,695.92 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,693.76 | ▲ 09:30 equity $9,693.76 vs yday $9,693.76 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 176 | $6.86 | $2.52 | — | $8,483.88 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-22.4; leftover $1211.72 | — |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 117 | $10.30 | $2.34 | — | $7,276.44 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-23.0; leftover $1211.72 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 36 | $33.14 | $2.10 | — | $6,081.30 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=-2.9; leftover $1211.72 | — |
| 2026-09-16 09:30 ET | **BUY** | `GFR` | 177 | $6.83 | $2.52 | — | $4,869.87 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list ohlc_hot; ret5=+11.2; leftover $1211.72 | — |
| 2026-09-16 09:30 ET | **BUY** | `HQ` | 94 | $12.89 | $2.27 | — | $3,655.94 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=-18.2; leftover $1211.72 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 52 | $23.29 | $2.15 | — | $2,442.71 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+16.1; leftover $1211.72 | — |
| 2026-09-16 09:30 ET | **BUY** | `DMRA` | 48 | $24.88 | $2.13 | — | $1,246.34 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-15.2; leftover $1211.72 | — |
| 2026-09-16 09:30 ET | **BUY** | `RVTY` | 8 | $140.88 | $2.01 | — | $117.29 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,ohlc_hot; 🔵; ret5=+10.3; leftover $1211.72 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.29 | ▼ close $9,608.97 vs 09:30 $9,693.76 (session -66.75) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.29 | ▲ 09:30 equity $9,722.71 vs yday $9,608.97 (+113.74) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `PLAY` | 176 | $6.96 | $2.56 | $+12.52 | $1,339.69 | ▲ +12.52 after sell → book $9,720.15; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `GFR` | 177 | $6.48 | $2.56 | $-67.03 | $2,484.09 | ▼ -67.03 after sell → book $9,717.59; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `HQ` | 94 | $13.56 | $2.30 | $+58.41 | $3,756.43 | ▲ +58.41 after sell → book $9,715.29; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 52 | $24.09 | $2.17 | $+37.29 | $5,006.95 | ▲ +37.29 after sell → book $9,713.13; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `DMRA` | 48 | $24.96 | $2.15 | $-0.45 | $6,202.87 | ▼ -0.45 after sell → book $9,710.97; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RVTY` | 8 | $147.61 | $2.03 | $+49.79 | $7,381.72 | ▲ +49.79 after sell → book $9,708.94; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 54 | $22.46 | $2.15 | — | $6,166.73 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $1230.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 5 | $238.60 | $2.00 | — | $4,971.72 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-11.6; leftover $1230.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `ALMU` | 109 | $11.21 | $2.32 | — | $3,747.51 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list earn_react; ret5=+1.0; leftover $1230.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 47 | $25.95 | $2.13 | — | $2,525.73 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1230.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMRX` | 66 | $18.56 | $2.19 | — | $1,298.58 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer; 🔵; ret5=+4.8; leftover $1230.29 | — |
| 2026-09-17 09:30 ET | **BUY** | `BTGO` | 187 | $6.56 | $2.55 | — | $69.31 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; ret5=-14.6; leftover $1230.29 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.31 | ▲ close $9,771.22 vs 09:30 $9,722.71 (session +75.62) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.31 | ▲ 09:30 equity $9,832.11 vs yday $9,771.22 (+60.89) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `ALHC` | 117 | $8.68 | $2.37 | $-194.25 | $1,082.50 | ▼ -194.25 after sell → book $9,829.74; vs 09:30 mark -2.37 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 36 | $39.50 | $2.12 | $+224.74 | $2,502.38 | ▲ +224.74 after sell → book $9,827.62; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 54 | $21.30 | $2.17 | $-66.96 | $3,650.41 | ▼ -66.96 after sell → book $9,825.45; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 5 | $236.80 | $2.02 | $-13.03 | $4,832.39 | ▼ -13.03 after sell → book $9,823.43; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMRX` | 66 | $18.12 | $2.21 | $-33.44 | $6,026.10 | ▼ -33.44 after sell → book $9,821.22; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BTGO` | 187 | $6.94 | $2.59 | $+65.92 | $7,321.28 | ▲ +65.92 after sell → book $9,818.62; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `GNRC` | 5 | $209.52 | $2.00 | — | $6,271.68 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list flatten,yday_gainer,yday_mover; 🔵; ret5=+14.1; leftover $1220.21 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 41 | $29.32 | $2.11 | — | $5,067.45 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1220.21 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 161 | $7.54 | $2.47 | — | $3,851.84 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_mover; 🔵; ret5=-20.9; leftover $1220.21 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 82 | $14.79 | $2.24 | — | $2,636.82 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1220.21 | — |
| 2026-09-18 09:30 ET | **BUY** | `SECZ` | 130 | $9.32 | $2.38 | — | $1,422.84 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; 🔵; ret5=+11.1; leftover $1220.21 | — |
| 2026-09-18 09:30 ET | **BUY** | `USDE` | 127 | $9.54 | $2.37 | — | $208.89 | — | Theme Radar Clock-B oppset universe (flagged T−1 names; research; not KEEP); gate oppset=True; rank opp_rvol; list yday_gainer,yday_mover; ret5=+15.8; leftover $1220.21 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $208.89 | ▲ close $10,090.07 vs 09:30 $9,832.11 (session +285.02) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.89 | ▲ 09:30 equity $10,652.91 vs yday $10,090.07 (+562.84) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `ALMU` | 109 | $13.12 | $2.35 | $+204.07 | $1,637.17 | ▲ +204.07 after sell → book $10,650.56; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ARQT` | 47 | $25.57 | $2.15 | $-22.14 | $2,836.81 | ▼ -22.14 after sell → book $10,648.41; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GNRC` | 5 | $210.00 | $2.02 | $-1.63 | $3,884.78 | ▼ -1.63 after sell → book $10,646.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 41 | $29.43 | $2.13 | $+0.26 | $5,089.28 | ▲ +0.26 after sell → book $10,644.25; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 161 | $7.36 | $2.51 | $-33.16 | $6,271.73 | ▼ -33.16 after sell → book $10,641.74; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 82 | $14.58 | $2.26 | $-21.72 | $7,465.03 | ▼ -21.72 after sell → book $10,639.48; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SECZ` | 130 | $11.67 | $2.41 | $+300.71 | $8,979.72 | ▲ +300.71 after sell → book $10,637.07; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `USDE` | 127 | $13.05 | $2.41 | $+440.99 | $10,634.66 | ▲ +440.99 after sell → book $10,634.66; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,634.66 | ▲ close $10,634.66 vs 09:30 $10,652.91 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,634.66 | ▲ 09:30 equity $10,634.66 vs yday $10,634.66 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,634.66 | ▲ close $10,634.66 vs 09:30 $10,634.66 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,634.66 | ▲ 09:30 equity $10,634.66 vs yday $10,634.66 (+0.00) | — | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,634.66 | ▲ close $10,634.66 vs 09:30 $10,634.66 (session +0.00) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,634.66 | ▲ 09:30 equity $10,634.66 vs yday $10,634.66 (+0.00) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,634.66 | ▲ close $10,634.66 vs 09:30 $10,634.66 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,547.55 | ▲ 09:30 equity $9,547.55 vs yday $9,547.55 (+0.00) | 09:30 open · cash $9,547.55 · no holdings · equity $9,547.55 vs prior close $9,547.55 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,547.55 | ▲ close $9,547.55 vs 09:30 $9,547.55 (session +0.00) | 16:00 close · cash $9,547.55 · no lots left · equity $9,547.55. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZIM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EYPT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `FN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PAAS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ARCT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PCG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MNSO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `REAX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AGCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ENOV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CNH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `RARE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ABTC` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `DPRO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NVS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CHA` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `TTAN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `IONS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WLTH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ODD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TYRA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `EVMN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ACVA` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ATEC` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NMAX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `RUM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `GFR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `DBI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `HQ` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
