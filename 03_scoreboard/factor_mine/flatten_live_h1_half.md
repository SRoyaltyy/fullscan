# Factor mine action — `flatten_live_h1_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **+4.55%** ($10,455) · signal-only (no cash/fees) was +4.99%. Starts YES **7/17**. Fills 32 · skips 0 · realized $+454.65.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,454.66.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-17 | +2.25 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-18 | -6.20 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-6.20 |
| 2026-08-19 | -7.20 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-7.20 |
| 2026-08-20 | +1.12 | $10,000.00 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $5,141.88 | $4,953.62 | $10,095.50 | AG×30, BHP×6, CDE×30, HDSN×108, IAG×31, KGC×21, NFGC×357, WPM×4 | BUY AG x30 @ 20.55; BUY BHP x6 @ 91.01; BUY CDE x30 @ 20.65; BUY HDSN x108 @ 5.77; BUY IAG x31 @ 19.63; BUY KGC x21 @ 29.63; BUY NFGC x357 @ 1.75; BUY WPM x4 @ 144.54 |
| 2026-08-21 | +3.25 | $5,141.88 | AG×30, BHP×6, CDE×30, HDSN×108, IAG×31, KGC×21, NFGC×357, WPM×4 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $5,374.71 | $4,937.36 | $10,312.07 | AU×5, AUPH×37, AEM×2, ARCT×57, AUTL×258, CRDL×330, CRSP×10, CYPH×483 | SELL AG (dropped from list after 1 sess (min 1)); SELL BHP (dropped from list after 1 sess (min 1)); SELL CDE (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); BUY AU x5 @ 119.43; BUY AUPH x37 @ 17.20; BUY AEM x2 @ 216.30; BUY ARCT x57 @ 11.13; BUY AUTL x258 @ 2.47; BUY CRDL x330 @ 1.93; BUY CRSP x10 @ 59.72; BUY CYPH x483 @ 1.32 |
| 2026-08-24 | -5.17 | $5,374.71 | AU×5, AUPH×37, AEM×2, ARCT×57, AUTL×258, CRDL×330, CRSP×10, CYPH×483 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,454.66 | $0.00 | $10,454.66 | — | SELL AU (dropped from list after 1 sess (min 1)); SELL AUPH (dropped from list after 1 sess (min 1)); SELL AEM (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL CRSP (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)) |
| 2026-08-25 | +1.80 | $10,454.66 | — | — | — | $10,454.66 | $0.00 | $10,454.66 | — | flat cash |
| 2026-08-26 | +2.02 | $10,454.66 | — | — | — | $10,454.66 | $0.00 | $10,454.66 | — | flat cash |
| 2026-08-27 | — | $10,454.66 | — | — | — | $10,454.66 | $0.00 | $10,454.66 | — | flat cash |
| 2026-08-28 | +0.75 | $10,454.66 | — | — | — | $10,454.66 | $0.00 | $10,454.66 | — | flat cash |
| 2026-08-31 | -5.85 | $10,454.66 | — | — | — | $10,454.66 | $0.00 | $10,454.66 | — | hard-red sit S=-5.85 |
| 2026-09-01 | -6.30 | $10,454.66 | — | — | — | $10,454.66 | $0.00 | $10,454.66 | — | hard-red sit S=-6.30 |
| 2026-09-02 | -3.83 | $10,454.66 | — | — | — | $10,454.66 | $0.00 | $10,454.66 | — | hard-red sit S=-3.83 |
| 2026-09-03 | -0.90 | $10,454.66 | — | — | — | $10,454.66 | $0.00 | $10,454.66 | — | flat cash |
| 2026-09-04 | — | $10,454.66 | — | — | — | $10,454.66 | $0.00 | $10,454.66 | — | flat cash |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-20 09:30 ET | **BUY** | `AG` | 30 | $20.55 | $2.08 | — | $9,381.42 | ▼ $9,997.92 (-2.08) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 6 | $91.01 | $2.01 | — | $8,833.35 | ▼ $9,995.91 (-4.09) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 30 | $20.65 | $2.08 | — | $8,211.77 | ▼ $9,993.83 (-6.17) | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 108 | $5.77 | $2.31 | — | $7,586.30 | ▼ $9,991.52 (-8.48) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 31 | $19.63 | $2.08 | — | $6,975.69 | ▼ $9,989.44 (-10.56) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 21 | $29.63 | $2.05 | — | $6,351.40 | ▼ $9,987.38 (-12.62) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 357 | $1.75 | $4.61 | — | $5,722.05 | ▼ $9,982.78 (-17.22) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,141.88 | ▼ $9,980.77 (-19.23) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $625.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 30 | $21.90 | $2.10 | $+36.32 | $5,796.78 | ▲ $10,225.63 (+225.63) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 6 | $95.72 | $2.03 | $+24.22 | $6,369.08 | ▲ $10,223.61 (+223.61) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 30 | $21.75 | $2.10 | $+28.82 | $7,019.48 | ▲ $10,221.51 (+221.51) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 108 | $5.67 | $2.34 | $-15.46 | $7,629.49 | ▲ $10,219.16 (+219.16) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 31 | $21.17 | $2.10 | $+43.55 | $8,283.66 | ▲ $10,217.06 (+217.06) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 21 | $32.17 | $2.07 | $+49.21 | $8,957.16 | ▲ $10,214.99 (+214.99) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 357 | $1.79 | $4.67 | $+5.00 | $9,591.51 | ▲ $10,210.31 (+210.31) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 4 | $154.70 | $2.02 | $+36.62 | $10,208.29 | ▲ $10,208.29 (+208.29) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 5 | $119.43 | $2.00 | — | $9,609.14 | ▲ $10,206.29 (+206.29) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+20.4; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 37 | $17.20 | $2.10 | — | $8,970.64 | ▲ $10,204.19 (+204.19) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 2 | $216.30 | $2.00 | — | $8,536.04 | ▲ $10,202.19 (+202.19) | deploy half leftover; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 57 | $11.13 | $2.16 | — | $7,899.47 | ▲ $10,200.03 (+200.03) | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 258 | $2.47 | $3.33 | — | $7,258.88 | ▲ $10,196.70 (+196.70) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 330 | $1.93 | $4.26 | — | $6,617.72 | ▲ $10,192.44 (+192.44) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 10 | $59.72 | $2.02 | — | $6,018.50 | ▲ $10,190.42 (+190.42) | deploy half leftover; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 483 | $1.32 | $6.23 | — | $5,374.71 | ▲ $10,184.19 (+184.19) | deploy half leftover; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $638.02 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 5 | $120.50 | $2.02 | $+1.32 | $5,975.19 | ▲ $10,477.04 (+477.04) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 37 | $16.60 | $2.12 | $-26.42 | $6,587.27 | ▲ $10,474.92 (+474.92) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 2 | $217.03 | $2.02 | $-2.55 | $7,019.31 | ▲ $10,472.90 (+472.90) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 57 | $13.26 | $2.18 | $+117.07 | $7,772.95 | ▲ $10,470.72 (+470.72) | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 258 | $2.36 | $3.38 | $-35.09 | $8,378.45 | ▲ $10,467.34 (+467.34) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 330 | $1.87 | $4.32 | $-28.38 | $8,991.23 | ▲ $10,463.02 (+463.02) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 10 | $58.79 | $2.04 | $-13.36 | $9,577.09 | ▲ $10,460.98 (+460.98) | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 483 | $1.83 | $6.32 | $+233.78 | $10,454.66 | ▲ $10,454.66 (+454.66) | dropped from list after 1 sess (min 1) | — |
