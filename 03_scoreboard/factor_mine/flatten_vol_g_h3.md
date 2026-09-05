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

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | BTBT, BETR | — | $10.00 | $9,828.63 | BTBT×3333, BETR×334 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $10.00 | BTBT×3333, BETR×334 | $9,641.94 | -186.69 | TMC | — | $1.81 | $9,864.51 | BTBT×3333, BETR×334, TMC×2 | 09:30 open · cash $10.00 (unchanged overnight, no fees) · equity $9,641.94 vs prior close $9,828.63 (-186.69) because holdings re-marked: BTBT×3333 yday $1.57 → 09:30 $1.52 -166.65; BETR×334 yday $13.73 → 09:30 $13.67 -20.04 |
| 2026-08-18 | -6.20 | $1.81 | BTBT×3333, BETR×334, TMC×2 | $9,554.21 | -310.30 | — | — | $1.81 | $9,201.20 | BTBT×3333, BETR×334, TMC×2 | 09:30 open · cash $1.81 (unchanged overnight, no fees) · equity $9,554.21 vs prior close $9,864.51 (-310.30) because holdings re-marked: BTBT×3333 yday $1.60 → 09:30 $1.54 -199.98; BETR×334 yday $13.54 → 09:30 $13.21 -110.22; TMC×2 yday $3.77 → 09:30 $3.72 -0.10 |
| 2026-08-19 | -7.20 | $1.81 | BTBT×3333, BETR×334, TMC×2 | $9,094.55 | -106.65 | — | BTBT, BETR | $9,038.70 | $9,046.64 | TMC×2 | 09:30 open · cash $1.81 (unchanged overnight, no fees) · equity $9,094.55 vs prior close $9,201.20 (-106.65) because holdings re-marked: BTBT×3333 yday $1.45 → 09:30 $1.42 -99.99; BETR×334 yday $13.05 → 09:30 $13.03 -6.68; TMC×2 yday $3.92 → 09:30 $3.93 +0.02 |
| 2026-08-20 | +1.12 | $9,038.70 | TMC×2 | $9,046.54 | -0.10 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | TMC | $173.17 | $9,233.36 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7 | 09:30 open · cash $9,038.70 (unchanged overnight, no fees) · equity $9,046.54 vs prior close $9,046.64 (-0.10) because holdings re-marked: TMC×2 yday $3.97 → 09:30 $3.92 -0.10 |
| 2026-08-21 | +3.25 | $173.17 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7 | $9,474.85 | +241.49 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $81.72 | $9,471.78 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | 09:30 open · cash $173.17 (unchanged overnight, no fees) · equity $9,474.85 vs prior close $9,233.36 (+241.49) because holdings re-marked: AG×55 yday $21.19 → 09:30 $21.90 +39.05; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×54 yday $21.11 → 09:30 $21.75 +34.56; HDSN×195 yday $5.57 → 09:30 $5.67 +19.50; IAG×57 yday $20.50 → 09:30 $21.17 +38.19; KGC×38 yday $31.43 → 09:30 $32.17 +28.12; NFGC×646 yday $1.75 → 09:30 $1.79 +25.84; WPM×7 yday $150.25 → 09:30 $154.70 +31.15 |
| 2026-08-24 | -5.17 | $81.72 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | $9,585.17 | +113.39 | — | — | $81.72 | $9,445.82 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | 09:30 open · cash $81.72 (unchanged overnight, no fees) · equity $9,585.17 vs prior close $9,471.78 (+113.39) because holdings re-marked: AG×55 yday $21.09 → 09:30 $21.47 +20.90; BHP×12 yday $97.03 → 09:30 $97.34 +3.72; CDE×54 yday $20.97 → 09:30 $21.26 +15.66; HDSN×195 yday $5.63 → 09:30 $5.69 +11.70; IAG×57 yday $21.14 → 09:30 $21.44 +17.10; KGC×38 yday $32.76 → 09:30 $33.21 +17.10; NFGC×646 yday $1.84 → 09:30 $1.86 +12.92; WPM×7 yday $157.78 → 09:30 $158.96 +8.26; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; AUTL×8 yday $2.41 → 09:30 $2.36 -0.40; CRDL×11 yday $1.86 → 09:30 $1.87 +0.11; CYPH×16 yday $1.42 → 09:30 $1.83 +6.56 |
| 2026-08-25 | +1.80 | $81.72 | AG×55, BHP×12, CDE×54, HDSN×195, IAG×57, KGC×38, NFGC×646, WPM×7, AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | $9,506.88 | +61.06 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $9,385.37 | $9,482.15 | AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | 09:30 open · cash $81.72 (unchanged overnight, no fees) · equity $9,506.88 vs prior close $9,445.82 (+61.06) because holdings re-marked: AG×55 yday $20.57 → 09:30 $20.73 +8.80; BHP×12 yday $96.66 → 09:30 $95.95 -8.52; CDE×54 yday $20.49 → 09:30 $20.85 +19.44; HDSN×195 yday $5.57 → 09:30 $5.53 -7.80; IAG×57 yday $21.36 → 09:30 $21.63 +15.39; KGC×38 yday $32.47 → 09:30 $32.76 +11.02; NFGC×646 yday $1.90 → 09:30 $1.91 +6.46; WPM×7 yday $158.00 → 09:30 $160.00 +14.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; AUTL×8 yday $2.38 → 09:30 $2.32 -0.48; CRDL×11 yday $1.80 → 09:30 $1.90 +1.10; CYPH×16 yday $1.64 → 09:30 $1.70 +0.96 |
| 2026-08-26 | +2.02 | $9,385.37 | AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | $9,482.15 | -0.00 | — | — | $9,385.37 | $9,483.08 | AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | 09:30 open · cash $9,385.37 (unchanged overnight, no fees) · equity $9,482.15 vs prior close $9,482.15 (-0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; AUTL×8 yday $2.34 → 09:30 $2.34 +0.00; CRDL×11 yday $1.90 → 09:30 $1.90 +0.00; CYPH×16 yday $1.64 → 09:30 $1.64 +0.00 |
| 2026-08-27 | — | $9,385.37 | AUPH×1, ARCT×1, AUTL×8, CRDL×11, CYPH×16 | $9,484.53 | +1.45 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $9,483.33 | $9,483.33 | — | 09:30 open · cash $9,385.37 (unchanged overnight, no fees) · equity $9,484.53 vs prior close $9,483.08 (+1.45) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; AUTL×8 yday $2.34 → 09:30 $2.41 +0.56; CRDL×11 yday $1.90 → 09:30 $2.03 +1.43; CYPH×16 yday $1.64 → 09:30 $1.60 -0.64 |
| 2026-08-28 | +0.75 | $9,483.33 | — | $9,483.33 | -0.00 | — | — | $9,483.33 | $9,483.33 | — | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $9,483.33 | — | $9,483.33 | -0.00 | — | — | $9,483.33 | $9,483.33 | — | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-01 | -6.30 | $9,483.33 | — | $9,483.33 | -0.00 | — | — | $9,483.33 | $9,483.33 | — | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $9,483.33 | — | $9,483.33 | -0.00 | — | — | $9,483.33 | $9,483.33 | — | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $9,483.33 | — | $9,483.33 | -0.00 | RVTY | — | $35.61 | $9,856.11 | RVTY×75 | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $35.61 | RVTY×75 | $9,969.36 | +113.25 | CABA | — | $2.59 | $9,831.16 | RVTY×75, CABA×9 | 09:30 open · cash $35.61 (unchanged overnight, no fees) · equity $9,969.36 vs prior close $9,856.11 (+113.25) because holdings re-marked: RVTY×75 yday $130.94 → 09:30 $132.45 +113.25 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3333 | $1.50 | $43.00 | — | $4,957.50 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 334 | $14.80 | $4.31 | — | $10.00 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-9.9; leftover $5000.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.00 | ▼ 09:30 equity $9,641.94 vs yday $9,828.63 (-186.69) | 09:30 open · cash $10.00 (unchanged overnight, no fees) · equity $9,641.94 vs prior close $9,828.63 (-186.69) because holdings re-marked: BTBT×3333 yday $1.57 → 09:30 $1.52 -166.65; BETR×334 yday $13.73 → 09:30 $13.67 -20.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $1.81 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $10.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.81 | ▼ 09:30 equity $9,554.21 vs yday $9,864.51 (-310.30) | 09:30 open · cash $1.81 (unchanged overnight, no fees) · equity $9,554.21 vs prior close $9,864.51 (-310.30) because holdings re-marked: BTBT×3333 yday $1.60 → 09:30 $1.54 -199.98; BETR×334 yday $13.54 → 09:30 $13.21 -110.22; TMC×2 yday $3.77 → 09:30 $3.72 -0.10 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.81 | ▼ 09:30 equity $9,094.55 vs yday $9,201.20 (-106.65) | 09:30 open · cash $1.81 (unchanged overnight, no fees) · equity $9,094.55 vs prior close $9,201.20 (-106.65) because holdings re-marked: BTBT×3333 yday $1.45 → 09:30 $1.42 -99.99; BETR×334 yday $13.05 → 09:30 $13.03 -6.68; TMC×2 yday $3.92 → 09:30 $3.93 +0.02 | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 3333 | $1.42 | $43.59 | $-353.22 | $4,691.08 | ▼ -353.22 after sell → book $9,050.96; vs 09:30 mark -43.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 334 | $13.03 | $4.40 | $-599.89 | $9,038.70 | ▼ -599.89 after sell → book $9,046.56; vs 09:30 mark -4.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,038.70 | ▼ 09:30 equity $9,046.54 vs yday $9,046.64 (-0.10) | 09:30 open · cash $9,038.70 (unchanged overnight, no fees) · equity $9,046.54 vs prior close $9,046.64 (-0.10) because holdings re-marked: TMC×2 yday $3.97 → 09:30 $3.92 -0.10 | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 2 | $3.92 | $0.10 | $-0.45 | $9,046.44 | ▼ -0.45 after sell → book $9,046.44; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 55 | $20.55 | $2.15 | — | $7,914.03 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $6,819.89 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 54 | $20.65 | $2.15 | — | $5,702.64 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 195 | $5.77 | $2.58 | — | $4,574.91 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 57 | $19.63 | $2.16 | — | $3,453.84 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 38 | $29.63 | $2.10 | — | $2,325.80 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 646 | $1.75 | $8.33 | — | $1,186.96 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $173.17 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1130.80 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.17 | ▲ 09:30 equity $9,474.85 vs yday $9,233.36 (+241.49) | 09:30 open · cash $173.17 (unchanged overnight, no fees) · equity $9,474.85 vs prior close $9,233.36 (+241.49) because holdings re-marked: AG×55 yday $21.19 → 09:30 $21.90 +39.05; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×54 yday $21.11 → 09:30 $21.75 +34.56; HDSN×195 yday $5.57 → 09:30 $5.67 +19.50; IAG×57 yday $20.50 → 09:30 $21.17 +38.19; KGC×38 yday $31.43 → 09:30 $32.17 +28.12; NFGC×646 yday $1.75 → 09:30 $1.79 +25.84; WPM×7 yday $150.25 → 09:30 $154.70 +31.15 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $155.80 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $144.55 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 8 | $2.47 | $0.22 | — | $124.57 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 11 | $1.93 | $0.25 | — | $103.09 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 16 | $1.32 | $0.26 | — | $81.72 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $21.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.72 | ▲ 09:30 equity $9,585.17 vs yday $9,471.78 (+113.39) | 09:30 open · cash $81.72 (unchanged overnight, no fees) · equity $9,585.17 vs prior close $9,471.78 (+113.39) because holdings re-marked: AG×55 yday $21.09 → 09:30 $21.47 +20.90; BHP×12 yday $97.03 → 09:30 $97.34 +3.72; CDE×54 yday $20.97 → 09:30 $21.26 +15.66; HDSN×195 yday $5.63 → 09:30 $5.69 +11.70; IAG×57 yday $21.14 → 09:30 $21.44 +17.10; KGC×38 yday $32.76 → 09:30 $33.21 +17.10; NFGC×646 yday $1.84 → 09:30 $1.86 +12.92; WPM×7 yday $157.78 → 09:30 $158.96 +8.26; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; AUTL×8 yday $2.41 → 09:30 $2.36 -0.40; CRDL×11 yday $1.86 → 09:30 $1.87 +0.11; CYPH×16 yday $1.42 → 09:30 $1.83 +6.56 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.72 | ▲ 09:30 equity $9,506.88 vs yday $9,445.82 (+61.06) | 09:30 open · cash $81.72 (unchanged overnight, no fees) · equity $9,506.88 vs prior close $9,445.82 (+61.06) because holdings re-marked: AG×55 yday $20.57 → 09:30 $20.73 +8.80; BHP×12 yday $96.66 → 09:30 $95.95 -8.52; CDE×54 yday $20.49 → 09:30 $20.85 +19.44; HDSN×195 yday $5.57 → 09:30 $5.53 -7.80; IAG×57 yday $21.36 → 09:30 $21.63 +15.39; KGC×38 yday $32.47 → 09:30 $32.76 +11.02; NFGC×646 yday $1.90 → 09:30 $1.91 +6.46; WPM×7 yday $158.00 → 09:30 $160.00 +14.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; AUTL×8 yday $2.38 → 09:30 $2.32 -0.48; CRDL×11 yday $1.80 → 09:30 $1.90 +1.10; CYPH×16 yday $1.64 → 09:30 $1.70 +0.96 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 55 | $20.73 | $2.17 | $+5.57 | $1,219.69 | ▲ +5.57 after sell → book $9,504.70; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 12 | $95.95 | $2.05 | $+55.21 | $2,369.04 | ▲ +55.21 after sell → book $9,502.65; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 54 | $20.85 | $2.17 | $+6.48 | $3,492.77 | ▲ +6.48 after sell → book $9,500.48; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 195 | $5.53 | $2.62 | $-51.99 | $4,568.51 | ▼ -51.99 after sell → book $9,497.87; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 57 | $21.63 | $2.18 | $+109.66 | $5,799.23 | ▲ +109.66 after sell → book $9,495.68; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 38 | $32.76 | $2.12 | $+114.71 | $7,041.99 | ▲ +114.71 after sell → book $9,493.56; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 646 | $1.91 | $8.45 | $+86.58 | $8,267.40 | ▲ +86.58 after sell → book $9,485.11; vs 09:30 mark -8.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 7 | $160.00 | $2.03 | $+104.18 | $9,385.37 | ▲ +104.18 after sell → book $9,483.08; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,385.37 | ▲ 09:30 equity $9,482.15 vs yday $9,482.15 (-0.00) | 09:30 open · cash $9,385.37 (unchanged overnight, no fees) · equity $9,482.15 vs prior close $9,482.15 (-0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; AUTL×8 yday $2.34 → 09:30 $2.34 +0.00; CRDL×11 yday $1.90 → 09:30 $1.90 +0.00; CYPH×16 yday $1.64 → 09:30 $1.64 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,385.37 | ▲ 09:30 equity $9,484.53 vs yday $9,483.08 (+1.45) | 09:30 open · cash $9,385.37 (unchanged overnight, no fees) · equity $9,484.53 vs prior close $9,483.08 (+1.45) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; AUTL×8 yday $2.34 → 09:30 $2.41 +0.56; CRDL×11 yday $1.90 → 09:30 $2.03 +1.43; CYPH×16 yday $1.64 → 09:30 $1.60 -0.64 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $9,401.78 | ▼ -0.96 after sell → book $9,484.34; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $9,416.95 | ▲ +3.93 after sell → book $9,484.16; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 8 | $2.41 | $0.24 | $-0.94 | $9,436.00 | ▼ -0.94 after sell → book $9,483.93; vs 09:30 mark -0.23 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 11 | $2.03 | $0.28 | $+0.58 | $9,458.05 | ▲ +0.58 after sell → book $9,483.65; vs 09:30 mark -0.28 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 16 | $1.60 | $0.32 | $+3.90 | $9,483.33 | ▲ +3.90 after sell → book $9,483.33; vs 09:30 mark -0.32 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,483.33 | ▲ 09:30 equity $9,483.33 vs yday $9,483.33 (-0.00) | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,483.33 | ▲ 09:30 equity $9,483.33 vs yday $9,483.33 (-0.00) | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,483.33 | ▲ 09:30 equity $9,483.33 vs yday $9,483.33 (-0.00) | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,483.33 | ▲ 09:30 equity $9,483.33 vs yday $9,483.33 (-0.00) | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,483.33 | ▲ 09:30 equity $9,483.33 vs yday $9,483.33 (-0.00) | 09:30 open · cash $9,483.33 · no holdings · equity $9,483.33 vs prior close $9,483.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 75 | $125.94 | $2.21 | — | $35.61 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $9483.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.61 | ▲ 09:30 equity $9,969.36 vs yday $9,856.11 (+113.25) | 09:30 open · cash $35.61 (unchanged overnight, no fees) · equity $9,969.36 vs prior close $9,856.11 (+113.25) because holdings re-marked: RVTY×75 yday $130.94 → 09:30 $132.45 +113.25 | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 9 | $3.63 | $0.35 | — | $2.59 | — | flatten wish-list ∩ vol🟢; gate vol=good; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $35.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |

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
