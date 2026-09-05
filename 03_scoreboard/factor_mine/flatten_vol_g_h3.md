# Factor mine action — `flatten_vol_g_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · flatten wish-list ∩ vol🟢

Cash book **-1.69%** ($9,831) · signal-only (no cash/fees) was +0.40%. Starts YES **14/17**. Fills 34 · skips 43 · realized $-516.65.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $2.59.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | — | — | $10,000.00 | $0.00 | $10,000.00 | — | flat cash |
| 2026-08-14 | +5.50 | $10,000.00 | — | BTBT, BETR | — | $10.00 | $9,818.63 | $9,828.63 | BTBT×3333, BETR×334 | BUY BTBT x3333 @ 1.50; BUY BETR x334 @ 14.80 |
| 2026-08-17 | +2.25 | $10.00 | BTBT×3333, BETR×334 | TMC | — | $1.81 | $9,862.70 | $9,864.51 | BTBT×3333, BETR×334, TMC×2 | BUY TMC x2 @ 4.05 |
| 2026-08-18 | -6.20 | $1.81 | BTBT×3333, BETR×334, TMC×2 | — | — | $1.81 | $9,199.39 | $9,201.20 | BTBT×3333, BETR×334, TMC×2 | hard-red sit S=-6.20 |
| 2026-08-19 | -7.20 | $1.81 | BTBT×3333, BETR×334, TMC×2 | — | BTBT, BETR | $9,038.70 | $7.94 | $9,046.64 | TMC×2 | SELL BTBT (dropped from list after 3 sess (min 3)); SELL BETR (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,038.70 | TMC×2 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | TMC | $173.17 | $9,060.19 | $9,233.36 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7 | SELL TMC (dropped from list after 3 sess (min 3)); BUY AG x55 @ 20.55; BUY BHP x12 @ 91.01; BUY CDE x54 @ 20.65; BUY HDSN x195 @ 5.77; BUY IAG x57 @ 19.63; BUY KGC x38 @ 29.63; BUY NFGC x646 @ 1.75; BUY WPM x7 @ 144.54 |
| 2026-08-21 | +3.25 | $173.17 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $81.72 | $9,390.06 | $9,471.78 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | BUY AUPH x1 @ 17.20; BUY ARCT x1 @ 11.13; BUY AUTL x8 @ 2.47; BUY CRDL x11 @ 1.93; BUY CYPH x16 @ 1.32 |
| 2026-08-24 | -5.17 | $81.72 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | — | — | $81.72 | $9,364.10 | $9,445.82 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $81.72 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $9,385.37 | $96.78 | $9,482.15 | AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | SELL AG (dropped from list after 3 sess (min 3)); SELL BHP (dropped from list after 3 sess (min 3)); SELL CDE (dropped from list after 3 sess (min 3)); SELL HDSN (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); SELL KGC (dropped from list after 3 sess (min 3)); SELL NFGC (dropped from list after 3 sess (min 3)); SELL WPM (dropped from list after 3 sess (min 3)) |
| 2026-08-26 | +2.02 | $9,385.37 | AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | — | — | $9,385.37 | $97.71 | $9,483.08 | AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | hold AUPH,ARCT,AUTL,CRDL,CYPH |
| 2026-08-27 | — | $9,385.37 | AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $9,483.33 | $0.00 | $9,483.33 | — | SELL AUPH (dropped from list after 4 sess (min 3)); SELL ARCT (dropped from list after 4 sess (min 3)); SELL AUTL (dropped from list after 4 sess (min 3)); SELL CRDL (dropped from list after 4 sess (min 3)); SELL CYPH (dropped from list after 4 sess (min 3)) |
| 2026-08-28 | +0.75 | $9,483.33 | — | — | — | $9,483.33 | $0.00 | $9,483.33 | — | flat cash |
| 2026-08-31 | -5.85 | $9,483.33 | — | — | — | $9,483.33 | $0.00 | $9,483.33 | — | hard-red sit S=-5.85 |
| 2026-09-01 | -6.30 | $9,483.33 | — | — | — | $9,483.33 | $0.00 | $9,483.33 | — | hard-red sit S=-6.30 |
| 2026-09-02 | -3.83 | $9,483.33 | — | — | — | $9,483.33 | $0.00 | $9,483.33 | — | hard-red sit S=-3.83 |
| 2026-09-03 | -0.90 | $9,483.33 | — | RVTY | — | $35.61 | $9,820.50 | $9,856.11 | RVTY×75 | BUY RVTY x75 @ 125.94 |
| 2026-09-04 | — | $35.61 | RVTY×75 | CABA | — | $2.59 | $9,828.57 | $9,831.16 | RVTY×75, CABA×9 | BUY CABA x9 @ 3.63 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3333 | $1.50 | $43.00 | — | $4,957.50 | ▼ $9,957.00 (-43.00) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 334 | $14.80 | $4.31 | — | $10.00 | ▼ $9,952.70 (-47.30) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-9.9; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $1.81 | ▼ $9,641.85 (-358.15) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $10.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 3333 | $1.42 | $43.59 | $-353.22 | $4,691.08 | ▼ $9,050.96 (-949.04) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 334 | $13.03 | $4.40 | $-599.89 | $9,038.70 | ▼ $9,046.56 (-953.44) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 2 | $3.92 | $0.10 | $-0.45 | $9,046.44 | ▼ $9,046.44 (-953.56) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 55 | $20.55 | $2.15 | — | $7,914.03 | ▼ $9,044.28 (-955.72) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $6,819.89 | ▼ $9,042.26 (-957.74) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 54 | $20.65 | $2.15 | — | $5,702.64 | ▼ $9,040.11 (-959.89) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 195 | $5.77 | $2.58 | — | $4,574.91 | ▼ $9,037.53 (-962.47) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 57 | $19.63 | $2.16 | — | $3,453.84 | ▼ $9,035.37 (-964.63) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 38 | $29.63 | $2.10 | — | $2,325.80 | ▼ $9,033.27 (-966.73) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 646 | $1.75 | $8.33 | — | $1,186.96 | ▼ $9,024.93 (-975.07) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $173.17 | ▼ $9,022.92 (-977.08) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $155.80 | ▼ $9,474.68 (-525.32) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $144.55 | ▼ $9,474.56 (-525.44) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 8 | $2.47 | $0.22 | — | $124.57 | ▼ $9,474.34 (-525.66) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 11 | $1.93 | $0.25 | — | $103.09 | ▼ $9,474.09 (-525.91) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 16 | $1.32 | $0.26 | — | $81.72 | ▼ $9,473.84 (-526.16) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 55 | $20.73 | $2.17 | $+5.57 | $1,219.69 | ▼ $9,504.70 (-495.30) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 12 | $95.95 | $2.05 | $+55.21 | $2,369.04 | ▼ $9,502.65 (-497.35) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 54 | $20.85 | $2.17 | $+6.48 | $3,492.77 | ▼ $9,500.48 (-499.52) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 195 | $5.53 | $2.62 | $-51.99 | $4,568.51 | ▼ $9,497.87 (-502.13) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 57 | $21.63 | $2.18 | $+109.66 | $5,799.23 | ▼ $9,495.68 (-504.32) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 38 | $32.76 | $2.12 | $+114.71 | $7,041.99 | ▼ $9,493.56 (-506.44) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 646 | $1.91 | $8.45 | $+86.58 | $8,267.40 | ▼ $9,485.11 (-514.89) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 7 | $160.00 | $2.03 | $+104.18 | $9,385.37 | ▼ $9,483.08 (-516.92) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $9,401.78 | ▼ $9,484.34 (-515.66) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $9,416.95 | ▼ $9,484.16 (-515.84) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 8 | $2.41 | $0.24 | $-0.94 | $9,436.00 | ▼ $9,483.93 (-516.07) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 11 | $2.03 | $0.28 | $+0.58 | $9,458.05 | ▼ $9,483.65 (-516.35) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 16 | $1.60 | $0.32 | $+3.90 | $9,483.33 | ▼ $9,483.33 (-516.67) | dropped from list after 4 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 75 | $125.94 | $2.21 | — | $35.61 | ▼ $9,481.11 (-518.89) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $9483.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 9 | $3.63 | $0.35 | — | $2.59 | ▼ $9,969.01 (-30.99) | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $35.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 21.65 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 21.65 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 21.65 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RVTY` | 75 | 2026-09-03 @ $125.94 | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $9483.33 |
| `CABA` | 9 | 2026-09-04 @ $3.63 | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $35.61 |
