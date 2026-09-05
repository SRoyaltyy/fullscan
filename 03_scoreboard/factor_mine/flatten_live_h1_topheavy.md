# Factor mine action — `flatten_live_h1_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

Side **long** · universe `flatten` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **+8.83%** ($10,883) · signal-only (no cash/fees) was +4.99%. Starts YES **7/17**. Fills 32 · skips 0 · realized $+883.25.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** New buys only when the live flatten gate fires (green S, ≥5 priced BUYs, prior book). io/HOLD mornings sit.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,883.24.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-17 | +2.25 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-18 | -6.20 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-6.20 |
| 2026-08-19 | -7.20 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | hard-red sit S=-7.20 |
| 2026-08-20 | +1.12 | $10,000.00 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $219.78 | $10,011.94 | $10,231.72 | AG×194, BHP×9, CDE×41, HDSN×148, IAG×43, KGC×28, NFGC×489, WPM×5 | BUY AG x194 @ 20.55; BUY BHP x9 @ 91.01; BUY CDE x41 @ 20.65; BUY HDSN x148 @ 5.77; BUY IAG x43 @ 19.63; BUY KGC x28 @ 29.63; BUY NFGC x489 @ 1.75; BUY WPM x5 @ 144.54 |
| 2026-08-21 | +3.25 | $219.78 | AG×194, BHP×9, CDE×41, HDSN×148, IAG×43, KGC×28, NFGC×489, WPM×5 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $45.48 | $10,652.26 | $10,697.74 | AU×35, AUPH×52, AEM×4, ARCT×80, AUTL×364, CRDL×466, CRSP×15, CYPH×681 | SELL AG (dropped from list after 1 sess (min 1)); SELL BHP (dropped from list after 1 sess (min 1)); SELL CDE (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); BUY AU x35 @ 119.43; BUY AUPH x52 @ 17.20; BUY AEM x4 @ 216.30; BUY ARCT x80 @ 11.13; BUY AUTL x364 @ 2.47; BUY CRDL x466 @ 1.93; BUY CRSP x15 @ 59.72; BUY CYPH x681 @ 1.32 |
| 2026-08-24 | -5.17 | $45.48 | AU×35, AUPH×52, AEM×4, ARCT×80, AUTL×364, CRDL×466, CRSP×15, CYPH×681 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,883.24 | $0.00 | $10,883.24 | — | SELL AU (dropped from list after 1 sess (min 1)); SELL AUPH (dropped from list after 1 sess (min 1)); SELL AEM (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL CRSP (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)) |
| 2026-08-25 | +1.80 | $10,883.24 | — | — | — | $10,883.24 | $0.00 | $10,883.24 | — | flat cash |
| 2026-08-26 | +2.02 | $10,883.24 | — | — | — | $10,883.24 | $0.00 | $10,883.24 | — | flat cash |
| 2026-08-27 | — | $10,883.24 | — | — | — | $10,883.24 | $0.00 | $10,883.24 | — | flat cash |
| 2026-08-28 | +0.75 | $10,883.24 | — | — | — | $10,883.24 | $0.00 | $10,883.24 | — | flat cash |
| 2026-08-31 | -5.85 | $10,883.24 | — | — | — | $10,883.24 | $0.00 | $10,883.24 | — | hard-red sit S=-5.85 |
| 2026-09-01 | -6.30 | $10,883.24 | — | — | — | $10,883.24 | $0.00 | $10,883.24 | — | hard-red sit S=-6.30 |
| 2026-09-02 | -3.83 | $10,883.24 | — | — | — | $10,883.24 | $0.00 | $10,883.24 | — | hard-red sit S=-3.83 |
| 2026-09-03 | -0.90 | $10,883.24 | — | — | — | $10,883.24 | $0.00 | $10,883.24 | — | flat cash |
| 2026-09-04 | — | $10,883.24 | — | — | — | $10,883.24 | $0.00 | $10,883.24 | — | flat cash |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-20 09:30 ET | **BUY** | `AG` | 194 | $20.55 | $2.57 | — | $6,010.73 | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $4000.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,189.62 | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 41 | $20.65 | $2.11 | — | $4,340.86 | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 148 | $5.77 | $2.43 | — | $3,484.46 | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 43 | $19.63 | $2.12 | — | $2,638.25 | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $1,806.54 | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 489 | $1.75 | $6.31 | — | $944.48 | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 5 | $144.54 | $2.00 | — | $219.78 | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $857.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 194 | $21.90 | $2.64 | $+256.69 | $4,465.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 9 | $95.72 | $2.04 | $+38.34 | $5,325.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 41 | $21.75 | $2.13 | $+40.85 | $6,214.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 148 | $5.67 | $2.47 | $-19.70 | $7,051.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 43 | $21.17 | $2.14 | $+61.96 | $7,959.66 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 28 | $32.17 | $2.09 | $+66.95 | $8,858.33 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 489 | $1.79 | $6.40 | $+6.85 | $9,727.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 5 | $154.70 | $2.02 | $+46.77 | $10,498.71 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 35 | $119.43 | $2.10 | — | $6,316.57 | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+20.4; leftover $4199.49 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 52 | $17.20 | $2.15 | — | $5,420.02 | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 4 | $216.30 | $2.00 | — | $4,552.82 | 40% to #1, rest split; list flatten,ohlc_hot,mover_buy; live flatten mover; 🔵; ⚪; ret5=+17.6; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 80 | $11.13 | $2.23 | — | $3,660.19 | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 364 | $2.47 | $4.70 | — | $2,756.42 | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 466 | $1.93 | $6.01 | — | $1,851.02 | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 15 | $59.72 | $2.04 | — | $953.19 | 40% to #1, rest split; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.6; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 681 | $1.32 | $8.78 | — | $45.48 | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $899.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 35 | $120.50 | $2.14 | $+33.22 | $4,260.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 52 | $16.60 | $2.17 | $-35.51 | $5,121.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 4 | $217.03 | $2.02 | $-1.10 | $5,987.98 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 80 | $13.26 | $2.25 | $+165.92 | $7,046.52 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 364 | $2.36 | $4.77 | $-49.50 | $7,900.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 466 | $1.87 | $6.10 | $-40.07 | $8,766.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 15 | $58.79 | $2.06 | $-18.04 | $9,645.91 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 681 | $1.83 | $8.91 | $+329.62 | $10,883.24 | dropped from list after 1 sess (min 1) | — |
