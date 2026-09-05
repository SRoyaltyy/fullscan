# Factor mine action — `union_break10_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ break10, no 🚨

Cash book **+2.04%** ($10,204) · signal-only (no cash/fees) was +6.99%. Starts YES **16/17**. Fills 65 · skips 127 · realized $-192.58.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `break_10=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $50.24.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | IREN | — | $19.54 | $9,732.46 | IREN×217 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $19.54 | IREN×217 | $9,587.07 | -145.39 | — | — | $19.54 | $9,580.56 | IREN×217 | 09:30 open · cash $19.54 (unchanged overnight, no fees) · equity $9,587.07 vs prior close $9,732.46 (-145.39) because holdings re-marked: IREN×217 yday $44.76 → 09:30 $44.09 -145.39 |
| 2026-08-17 | +2.25 | $19.54 | IREN×217 | $9,834.45 | +253.89 | NPWR | — | $17.60 | $9,762.63 | IREN×217, NPWR×1 | 09:30 open · cash $19.54 (unchanged overnight, no fees) · equity $9,834.45 vs prior close $9,580.56 (+253.89) because holdings re-marked: IREN×217 yday $44.06 → 09:30 $45.23 +253.89 |
| 2026-08-18 | -6.20 | $17.60 | IREN×217, NPWR×1 | $9,471.82 | -290.81 | — | IREN | $9,467.21 | $9,468.86 | NPWR×1 | 09:30 open · cash $17.60 (unchanged overnight, no fees) · equity $9,471.82 vs prior close $9,762.63 (-290.81) because holdings re-marked: IREN×217 yday $44.90 → 09:30 $43.56 -290.78; NPWR×1 yday $1.73 → 09:30 $1.70 -0.03 |
| 2026-08-19 | -7.20 | $9,467.21 | NPWR×1 | $9,468.91 | +0.05 | — | — | $9,467.21 | $9,468.88 | NPWR×1 | 09:30 open · cash $9,467.21 (unchanged overnight, no fees) · equity $9,468.91 vs prior close $9,468.86 (+0.05) because holdings re-marked: NPWR×1 yday $1.65 → 09:30 $1.70 +0.05 |
| 2026-08-20 | +1.12 | $9,467.21 | NPWR×1 | $9,468.85 | -0.03 | AG, BHP, CDE, IAG, KGC, NFGC, WPM, ABUS | NPWR | $59.33 | $9,673.19 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240 | 09:30 open · cash $9,467.21 (unchanged overnight, no fees) · equity $9,468.85 vs prior close $9,468.88 (-0.03) because holdings re-marked: NPWR×1 yday $1.67 → 09:30 $1.64 -0.03 |
| 2026-08-21 | +3.25 | $59.33 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240 | $10,012.21 | +339.02 | CYPH, ORBS, CAN, DFDV | — | $34.06 | $10,022.35 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240, CYPH×5, ORBS×8, CAN×25, DFDV×1 | 09:30 open · cash $59.33 (unchanged overnight, no fees) · equity $10,012.21 vs prior close $9,673.19 (+339.02) because holdings re-marked: AG×57 yday $21.19 → 09:30 $21.90 +40.47; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×676 yday $1.75 → 09:30 $1.79 +27.04; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×240 yday $4.77 → 09:30 $5.20 +103.20 |
| 2026-08-24 | -5.17 | $34.06 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240, CYPH×5, ORBS×8, CAN×25, DFDV×1 | $10,118.84 | +96.49 | — | — | $34.06 | $10,003.83 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240, CYPH×5, ORBS×8, CAN×25, DFDV×1 | 09:30 open · cash $34.06 (unchanged overnight, no fees) · equity $10,118.84 vs prior close $10,022.35 (+96.49) because holdings re-marked: AG×57 yday $21.09 → 09:30 $21.47 +21.66; BHP×13 yday $97.03 → 09:30 $97.34 +4.03; CDE×57 yday $20.97 → 09:30 $21.26 +16.53; IAG×60 yday $21.14 → 09:30 $21.44 +18.00; KGC×39 yday $32.76 → 09:30 $33.21 +17.55; NFGC×676 yday $1.84 → 09:30 $1.86 +13.52; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; ABUS×240 yday $5.21 → 09:30 $5.18 -7.20; CYPH×5 yday $1.42 → 09:30 $1.83 +2.05; ORBS×8 yday $0.88 → 09:30 $0.89 +0.08; CAN×25 yday $0.35 → 09:30 $0.38 +0.63; DFDV×1 yday $3.94 → 09:30 $4.15 +0.21 |
| 2026-08-25 | +1.80 | $34.06 | AG×57, BHP×13, CDE×57, IAG×60, KGC×39, NFGC×676, WPM×8, ABUS×240, CYPH×5, ORBS×8, CAN×25, DFDV×1 | $10,089.56 | +85.73 | MOS, INSP, RZLT, HCA, ALVO, DEFT, ASST | AG, BHP, CDE, IAG, KGC, NFGC, WPM, ABUS | $163.55 | $9,942.55 | CYPH×5, ORBS×8, CAN×25, DFDV×1, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×274, DEFT×2240, ASST×68 | 09:30 open · cash $34.06 (unchanged overnight, no fees) · equity $10,089.56 vs prior close $10,003.83 (+85.73) because holdings re-marked: AG×57 yday $20.57 → 09:30 $20.73 +9.12; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; CDE×57 yday $20.49 → 09:30 $20.85 +20.52; IAG×60 yday $21.36 → 09:30 $21.63 +16.20; KGC×39 yday $32.47 → 09:30 $32.76 +11.31; NFGC×676 yday $1.90 → 09:30 $1.91 +6.76; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; ABUS×240 yday $5.20 → 09:30 $5.26 +14.40; CYPH×5 yday $1.64 → 09:30 $1.70 +0.30; ORBS×8 yday $0.85 → 09:30 $0.85 +0.00; CAN×25 yday $0.37 → 09:30 $0.38 +0.25; DFDV×1 yday $4.19 → 09:30 $4.29 +0.10 |
| 2026-08-26 | +2.02 | $163.55 | CYPH×5, ORBS×8, CAN×25, DFDV×1, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×274, DEFT×2240, ASST×68 | $9,942.55 | -0.00 | — | — | $163.55 | $10,028.27 | CYPH×5, ORBS×8, CAN×25, DFDV×1, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×274, DEFT×2240, ASST×68 | 09:30 open · cash $163.55 (unchanged overnight, no fees) · equity $9,942.55 vs prior close $9,942.55 (-0.00) because holdings re-marked: CYPH×5 yday $1.64 → 09:30 $1.64 +0.00; ORBS×8 yday $0.84 → 09:30 $0.84 +0.00; CAN×25 yday $0.36 → 09:30 $0.36 +0.00; DFDV×1 yday $4.16 → 09:30 $4.16 +0.00; MOS×59 yday $23.75 → 09:30 $23.75 +0.00; INSP×23 yday $61.47 → 09:30 $61.47 +0.00; RZLT×274 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; ALVO×274 yday $5.25 → 09:30 $5.25 +0.00; DEFT×2240 yday $0.62 → 09:30 $0.62 +0.00; ASST×68 yday $20.20 → 09:30 $20.20 +0.00 |
| 2026-08-27 | — | $163.55 | CYPH×5, ORBS×8, CAN×25, DFDV×1, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×274, DEFT×2240, ASST×68 | $9,812.19 | -216.08 | — | CYPH, ORBS, CAN, DFDV | $191.81 | $9,830.03 | MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×274, DEFT×2240, ASST×68 | 09:30 open · cash $163.55 (unchanged overnight, no fees) · equity $9,812.19 vs prior close $10,028.27 (-216.08) because holdings re-marked: CYPH×5 yday $1.64 → 09:30 $1.60 -0.20; ORBS×8 yday $0.84 → 09:30 $0.80 -0.32; CAN×25 yday $0.36 → 09:30 $0.40 +1.00; DFDV×1 yday $4.16 → 09:30 $4.35 +0.19; MOS×59 yday $23.75 → 09:30 $24.84 +64.31; INSP×23 yday $61.47 → 09:30 $60.07 -32.20; RZLT×274 yday $5.29 → 09:30 $5.01 -76.72; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; ALVO×274 yday $5.25 → 09:30 $4.98 -73.98; DEFT×2240 yday $0.62 → 09:30 $0.60 -44.80; ASST×68 yday $20.20 → 09:30 $20.72 +35.36 |
| 2026-08-28 | +0.75 | $191.81 | MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×274, DEFT×2240, ASST×68 | $9,906.84 | +76.81 | ZYME, FIGR, NIQ, ERO, TRLV, CVI, VIRT | INSP, RZLT, HCA, ALVO, DEFT, ASST | $78.34 | $9,870.01 | MOS×59, ZYME×41, FIGR×32, NIQ×64, ERO×30, TRLV×106, CVI×30, VIRT×18 | 09:30 open · cash $191.81 (unchanged overnight, no fees) · equity $9,906.84 vs prior close $9,830.03 (+76.81) because holdings re-marked: MOS×59 yday $24.16 → 09:30 $24.00 -9.44; INSP×23 yday $61.80 → 09:30 $62.10 +6.90; RZLT×274 yday $5.04 → 09:30 $5.07 +8.22; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; ALVO×274 yday $4.91 → 09:30 $4.88 -8.22; DEFT×2240 yday $0.59 → 09:30 $0.60 +22.40; ASST×68 yday $21.50 → 09:30 $22.45 +64.60 |
| 2026-08-31 | -5.85 | $78.34 | MOS×59, ZYME×41, FIGR×32, NIQ×64, ERO×30, TRLV×106, CVI×30, VIRT×18 | $9,924.74 | +54.73 | — | MOS | $1,477.40 | $9,948.37 | ZYME×41, FIGR×32, NIQ×64, ERO×30, TRLV×106, CVI×30, VIRT×18 | 09:30 open · cash $78.34 (unchanged overnight, no fees) · equity $9,924.74 vs prior close $9,870.01 (+54.73) because holdings re-marked: MOS×59 yday $23.76 → 09:30 $23.75 -0.59; ZYME×41 yday $29.01 → 09:30 $28.27 -30.34; FIGR×32 yday $38.02 → 09:30 $35.50 -80.64; NIQ×64 yday $19.07 → 09:30 $19.20 +8.32; ERO×30 yday $39.82 → 09:30 $38.60 -36.60; TRLV×106 yday $11.03 → 09:30 $12.41 +146.28; CVI×30 yday $39.76 → 09:30 $41.76 +60.00; VIRT×18 yday $67.04 → 09:30 $66.39 -11.70 |
| 2026-09-01 | -6.30 | $1,477.40 | ZYME×41, FIGR×32, NIQ×64, ERO×30, TRLV×106, CVI×30, VIRT×18 | $9,923.62 | -24.75 | — | — | $1,477.40 | $9,850.13 | ZYME×41, FIGR×32, NIQ×64, ERO×30, TRLV×106, CVI×30, VIRT×18 | 09:30 open · cash $1,477.40 (unchanged overnight, no fees) · equity $9,923.62 vs prior close $9,948.37 (-24.75) because holdings re-marked: ZYME×41 yday $28.27 → 09:30 $29.32 +43.05; FIGR×32 yday $36.41 → 09:30 $36.80 +12.48; NIQ×64 yday $19.20 → 09:30 $19.06 -8.96; ERO×30 yday $38.49 → 09:30 $37.30 -35.70; TRLV×106 yday $12.41 → 09:30 $11.89 -55.12; CVI×30 yday $41.76 → 09:30 $42.86 +33.00; VIRT×18 yday $66.39 → 09:30 $65.64 -13.50 |
| 2026-09-02 | -3.83 | $1,477.40 | ZYME×41, FIGR×32, NIQ×64, ERO×30, TRLV×106, CVI×30, VIRT×18 | $9,797.02 | -53.11 | — | ZYME, FIGR, NIQ, ERO, CVI, VIRT | $8,561.07 | $9,805.51 | TRLV×106 | 09:30 open · cash $1,477.40 (unchanged overnight, no fees) · equity $9,797.02 vs prior close $9,850.13 (-53.11) because holdings re-marked: ZYME×41 yday $29.33 → 09:30 $29.32 -0.41; FIGR×32 yday $35.70 → 09:30 $35.46 -7.68; NIQ×64 yday $19.06 → 09:30 $19.00 -3.84; ERO×30 yday $36.01 → 09:30 $35.95 -1.80; TRLV×106 yday $11.89 → 09:30 $11.54 -37.10; CVI×30 yday $42.86 → 09:30 $42.94 +2.40; VIRT×18 yday $65.64 → 09:30 $65.38 -4.68 |
| 2026-09-03 | -0.90 | $8,561.07 | TRLV×106 | $9,809.75 | +4.24 | ATRC, RVTY, DEFT, ARCT, SID, NVAX, CAN, CDXS | TRLV | $62.25 | $9,865.32 | ATRC×24, RVTY×9, DEFT×1829, ARCT×74, SID×1066, NVAX×119, CAN×4086, CDXS×806 | 09:30 open · cash $8,561.07 (unchanged overnight, no fees) · equity $9,809.75 vs prior close $9,805.51 (+4.24) because holdings re-marked: TRLV×106 yday $11.74 → 09:30 $11.78 +4.24 |
| 2026-09-04 | — | $62.25 | ATRC×24, RVTY×9, DEFT×1829, ARCT×74, SID×1066, NVAX×119, CAN×4086, CDXS×806 | $10,223.92 | +358.60 | TRLV | — | $50.24 | $10,203.78 | ATRC×24, RVTY×9, DEFT×1829, ARCT×74, SID×1066, NVAX×119, CAN×4086, CDXS×806, TRLV×1 | 09:30 open · cash $62.25 (unchanged overnight, no fees) · equity $10,223.92 vs prior close $9,865.32 (+358.60) because holdings re-marked: ATRC×24 yday $52.59 → 09:30 $52.88 +6.96; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; DEFT×1829 yday $0.65 → 09:30 $0.65 +0.00; ARCT×74 yday $16.74 → 09:30 $16.77 +2.22; SID×1066 yday $1.17 → 09:30 $1.36 +202.54; NVAX×119 yday $10.32 → 09:30 $10.41 +10.71; CAN×4086 yday $0.31 → 09:30 $0.34 +122.58; CDXS×806 yday $1.48 → 09:30 $1.48 +0.00 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 217 | $45.98 | $2.80 | — | $19.54 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ⚪; ret5=+12.3; leftover $10000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.54 | ▼ 09:30 equity $9,587.07 vs yday $9,732.46 (-145.39) | 09:30 open · cash $19.54 (unchanged overnight, no fees) · equity $9,587.07 vs prior close $9,732.46 (-145.39) because holdings re-marked: IREN×217 yday $44.76 → 09:30 $44.09 -145.39 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.54 | ▲ 09:30 equity $9,834.45 vs yday $9,580.56 (+253.89) | 09:30 open · cash $19.54 (unchanged overnight, no fees) · equity $9,834.45 vs prior close $9,580.56 (+253.89) because holdings re-marked: IREN×217 yday $44.06 → 09:30 $45.23 +253.89 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 1 | $1.92 | $0.02 | — | $17.60 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $2.44 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.60 | ▼ 09:30 equity $9,471.82 vs yday $9,762.63 (-290.81) | 09:30 open · cash $17.60 (unchanged overnight, no fees) · equity $9,471.82 vs prior close $9,762.63 (-290.81) because holdings re-marked: IREN×217 yday $44.90 → 09:30 $43.56 -290.78; NPWR×1 yday $1.73 → 09:30 $1.70 -0.03 | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 217 | $43.56 | $2.91 | $-530.85 | $9,467.21 | ▼ -530.85 after sell → book $9,468.91; vs 09:30 mark -2.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,467.21 | ▲ 09:30 equity $9,468.91 vs yday $9,468.86 (+0.05) | 09:30 open · cash $9,467.21 (unchanged overnight, no fees) · equity $9,468.91 vs prior close $9,468.86 (+0.05) because holdings re-marked: NPWR×1 yday $1.65 → 09:30 $1.70 +0.05 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,467.21 | ▼ 09:30 equity $9,468.85 vs yday $9,468.88 (-0.03) | 09:30 open · cash $9,467.21 (unchanged overnight, no fees) · equity $9,468.85 vs prior close $9,468.88 (-0.03) because holdings re-marked: NPWR×1 yday $1.67 → 09:30 $1.64 -0.03 | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 1 | $1.64 | $0.04 | $-0.34 | $9,468.81 | ▼ -0.34 after sell → book $9,468.81; vs 09:30 mark -0.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,295.30 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,110.14 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $5,930.93 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $4,750.96 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,593.28 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 676 | $1.75 | $8.72 | — | $2,401.56 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,243.23 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 240 | $4.92 | $3.10 | — | $59.33 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1183.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.33 | ▲ 09:30 equity $10,012.21 vs yday $9,673.19 (+339.02) | 09:30 open · cash $59.33 (unchanged overnight, no fees) · equity $10,012.21 vs prior close $9,673.19 (+339.02) because holdings re-marked: AG×57 yday $21.19 → 09:30 $21.90 +40.47; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×676 yday $1.75 → 09:30 $1.79 +27.04; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×240 yday $4.77 → 09:30 $5.20 +103.20 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 5 | $1.32 | $0.08 | — | $52.65 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $7.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 8 | $0.86 | $0.09 | — | $45.64 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $7.42 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 25 | $0.29 | $0.15 | — | $38.15 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $7.42 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DFDV` | 1 | $4.04 | $0.04 | — | $34.06 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $7.42 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.06 | ▲ 09:30 equity $10,118.84 vs yday $10,022.35 (+96.49) | 09:30 open · cash $34.06 (unchanged overnight, no fees) · equity $10,118.84 vs prior close $10,022.35 (+96.49) because holdings re-marked: AG×57 yday $21.09 → 09:30 $21.47 +21.66; BHP×13 yday $97.03 → 09:30 $97.34 +4.03; CDE×57 yday $20.97 → 09:30 $21.26 +16.53; IAG×60 yday $21.14 → 09:30 $21.44 +18.00; KGC×39 yday $32.76 → 09:30 $33.21 +17.55; NFGC×676 yday $1.84 → 09:30 $1.86 +13.52; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; ABUS×240 yday $5.21 → 09:30 $5.18 -7.20; CYPH×5 yday $1.42 → 09:30 $1.83 +2.05; ORBS×8 yday $0.88 → 09:30 $0.89 +0.08; CAN×25 yday $0.35 → 09:30 $0.38 +0.63; DFDV×1 yday $3.94 → 09:30 $4.15 +0.21 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.06 | ▲ 09:30 equity $10,089.56 vs yday $10,003.83 (+85.73) | 09:30 open · cash $34.06 (unchanged overnight, no fees) · equity $10,089.56 vs prior close $10,003.83 (+85.73) because holdings re-marked: AG×57 yday $20.57 → 09:30 $20.73 +9.12; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; CDE×57 yday $20.49 → 09:30 $20.85 +20.52; IAG×60 yday $21.36 → 09:30 $21.63 +16.20; KGC×39 yday $32.47 → 09:30 $32.76 +11.31; NFGC×676 yday $1.90 → 09:30 $1.91 +6.76; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; ABUS×240 yday $5.20 → 09:30 $5.26 +14.40; CYPH×5 yday $1.64 → 09:30 $1.70 +0.30; ORBS×8 yday $0.85 → 09:30 $0.85 +0.00; CAN×25 yday $0.37 → 09:30 $0.38 +0.25; DFDV×1 yday $4.19 → 09:30 $4.29 +0.10 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 57 | $20.73 | $2.18 | $+5.92 | $1,213.49 | ▲ +5.92 after sell → book $10,087.38; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $2,458.79 | ▲ +60.14 after sell → book $10,085.33; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 57 | $20.85 | $2.18 | $+7.06 | $3,645.06 | ▲ +7.06 after sell → book $10,083.15; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 60 | $21.63 | $2.19 | $+115.64 | $4,940.67 | ▲ +115.64 after sell → book $10,080.96; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 39 | $32.76 | $2.13 | $+117.84 | $6,216.18 | ▲ +117.84 after sell → book $10,078.83; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 676 | $1.91 | $8.84 | $+90.60 | $7,498.50 | ▲ +90.60 after sell → book $10,069.99; vs 09:30 mark -8.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $8,776.47 | ▲ +119.63 after sell → book $10,067.96; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 240 | $5.26 | $3.15 | $+75.36 | $10,035.72 | ▲ +75.36 after sell → book $10,064.81; vs 09:30 mark -3.15 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 59 | $24.00 | $2.17 | — | $8,617.55 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ⚪; ret5=+13.0; leftover $1433.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 23 | $61.47 | $2.06 | — | $7,201.68 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ret5=+9.2; leftover $1433.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 274 | $5.23 | $3.53 | — | $5,765.13 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ret5=+10.7; leftover $1433.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $4,475.41 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; ret5=+6.1; leftover $1433.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 274 | $5.22 | $3.53 | — | $3,041.60 | — | union ∩ break10, no 🚨; gate break_10=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1433.67 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2240 | $0.64 | $21.06 | — | $1,586.94 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1433.67 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 68 | $20.90 | $2.19 | — | $163.55 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+47.9; leftover $1433.67 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $163.55 | ▲ 09:30 equity $9,942.55 vs yday $9,942.55 (-0.00) | 09:30 open · cash $163.55 (unchanged overnight, no fees) · equity $9,942.55 vs prior close $9,942.55 (-0.00) because holdings re-marked: CYPH×5 yday $1.64 → 09:30 $1.64 +0.00; ORBS×8 yday $0.84 → 09:30 $0.84 +0.00; CAN×25 yday $0.36 → 09:30 $0.36 +0.00; DFDV×1 yday $4.16 → 09:30 $4.16 +0.00; MOS×59 yday $23.75 → 09:30 $23.75 +0.00; INSP×23 yday $61.47 → 09:30 $61.47 +0.00; RZLT×274 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; ALVO×274 yday $5.25 → 09:30 $5.25 +0.00; DEFT×2240 yday $0.62 → 09:30 $0.62 +0.00; ASST×68 yday $20.20 → 09:30 $20.20 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $163.55 | ▼ 09:30 equity $9,812.19 vs yday $10,028.27 (-216.08) | 09:30 open · cash $163.55 (unchanged overnight, no fees) · equity $9,812.19 vs prior close $10,028.27 (-216.08) because holdings re-marked: CYPH×5 yday $1.64 → 09:30 $1.60 -0.20; ORBS×8 yday $0.84 → 09:30 $0.80 -0.32; CAN×25 yday $0.36 → 09:30 $0.40 +1.00; DFDV×1 yday $4.16 → 09:30 $4.35 +0.19; MOS×59 yday $23.75 → 09:30 $24.84 +64.31; INSP×23 yday $61.47 → 09:30 $60.07 -32.20; RZLT×274 yday $5.29 → 09:30 $5.01 -76.72; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; ALVO×274 yday $5.25 → 09:30 $4.98 -73.98; DEFT×2240 yday $0.62 → 09:30 $0.60 -44.80; ASST×68 yday $20.20 → 09:30 $20.72 +35.36 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 5 | $1.60 | $0.12 | $+1.20 | $171.43 | ▲ +1.20 after sell → book $9,812.07; vs 09:30 mark -0.12 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ORBS` | 8 | $0.80 | $0.11 | $-0.71 | $177.72 | ▼ -0.71 after sell → book $9,811.96; vs 09:30 mark -0.11 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAN` | 25 | $0.40 | $0.20 | $+2.31 | $187.53 | ▲ +2.31 after sell → book $9,811.77; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `DFDV` | 1 | $4.35 | $0.07 | $+0.20 | $191.81 | ▲ +0.20 after sell → book $9,811.70; vs 09:30 mark -0.07 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $191.81 | ▲ 09:30 equity $9,906.84 vs yday $9,830.03 (+76.81) | 09:30 open · cash $191.81 (unchanged overnight, no fees) · equity $9,906.84 vs prior close $9,830.03 (+76.81) because holdings re-marked: MOS×59 yday $24.16 → 09:30 $24.00 -9.44; INSP×23 yday $61.80 → 09:30 $62.10 +6.90; RZLT×274 yday $5.04 → 09:30 $5.07 +8.22; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; ALVO×274 yday $4.91 → 09:30 $4.88 -8.22; DEFT×2240 yday $0.59 → 09:30 $0.60 +22.40; ASST×68 yday $21.50 → 09:30 $22.45 +64.60 | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 23 | $62.10 | $2.08 | $+10.35 | $1,618.03 | ▲ +10.35 after sell → book $9,904.76; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 274 | $5.07 | $3.59 | $-50.97 | $3,003.62 | ▼ -50.97 after sell → book $9,901.17; vs 09:30 mark -3.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-17.91 | $4,275.43 | ▼ -17.91 after sell → book $9,899.15; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 274 | $4.88 | $3.59 | $-100.29 | $5,608.96 | ▼ -100.29 after sell → book $9,895.56; vs 09:30 mark -3.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DEFT` | 2240 | $0.60 | $20.54 | $-131.20 | $6,932.42 | ▼ -131.20 after sell → book $9,875.02; vs 09:30 mark -20.54 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ASST` | 68 | $22.45 | $2.22 | $+100.99 | $8,456.80 | ▲ +100.99 after sell → book $9,872.80; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 41 | $29.33 | $2.11 | — | $7,252.16 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1208.11 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 32 | $37.42 | $2.09 | — | $6,052.63 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_mover; ret5=+24.4; leftover $1208.11 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 64 | $18.79 | $2.18 | — | $4,847.89 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+7.6; leftover $1208.11 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 30 | $39.20 | $2.08 | — | $3,669.81 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+16.6; leftover $1208.11 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 106 | $11.38 | $2.31 | — | $2,461.22 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+15.0; leftover $1208.11 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CVI` | 30 | $40.04 | $2.08 | — | $1,257.94 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+12.1; leftover $1208.11 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 18 | $65.42 | $2.04 | — | $78.34 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; ret5=+13.2; leftover $1208.11 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.34 | ▲ 09:30 equity $9,924.74 vs yday $9,870.01 (+54.73) | 09:30 open · cash $78.34 (unchanged overnight, no fees) · equity $9,924.74 vs prior close $9,870.01 (+54.73) because holdings re-marked: MOS×59 yday $23.76 → 09:30 $23.75 -0.59; ZYME×41 yday $29.01 → 09:30 $28.27 -30.34; FIGR×32 yday $38.02 → 09:30 $35.50 -80.64; NIQ×64 yday $19.07 → 09:30 $19.20 +8.32; ERO×30 yday $39.82 → 09:30 $38.60 -36.60; TRLV×106 yday $11.03 → 09:30 $12.41 +146.28; CVI×30 yday $39.76 → 09:30 $41.76 +60.00; VIRT×18 yday $67.04 → 09:30 $66.39 -11.70 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 59 | $23.75 | $2.19 | $-19.11 | $1,477.40 | ▼ -19.11 after sell → book $9,922.55; vs 09:30 mark -2.19 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,477.40 | ▼ 09:30 equity $9,923.62 vs yday $9,948.37 (-24.75) | 09:30 open · cash $1,477.40 (unchanged overnight, no fees) · equity $9,923.62 vs prior close $9,948.37 (-24.75) because holdings re-marked: ZYME×41 yday $28.27 → 09:30 $29.32 +43.05; FIGR×32 yday $36.41 → 09:30 $36.80 +12.48; NIQ×64 yday $19.20 → 09:30 $19.06 -8.96; ERO×30 yday $38.49 → 09:30 $37.30 -35.70; TRLV×106 yday $12.41 → 09:30 $11.89 -55.12; CVI×30 yday $41.76 → 09:30 $42.86 +33.00; VIRT×18 yday $66.39 → 09:30 $65.64 -13.50 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,477.40 | ▼ 09:30 equity $9,797.02 vs yday $9,850.13 (-53.11) | 09:30 open · cash $1,477.40 (unchanged overnight, no fees) · equity $9,797.02 vs prior close $9,850.13 (-53.11) because holdings re-marked: ZYME×41 yday $29.33 → 09:30 $29.32 -0.41; FIGR×32 yday $35.70 → 09:30 $35.46 -7.68; NIQ×64 yday $19.06 → 09:30 $19.00 -3.84; ERO×30 yday $36.01 → 09:30 $35.95 -1.80; TRLV×106 yday $11.89 → 09:30 $11.54 -37.10; CVI×30 yday $42.86 → 09:30 $42.94 +2.40; VIRT×18 yday $65.64 → 09:30 $65.38 -4.68 | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 41 | $29.32 | $2.13 | $-4.66 | $2,677.39 | ▼ -4.66 after sell → book $9,794.89; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `FIGR` | 32 | $35.46 | $2.11 | $-66.91 | $3,810.00 | ▼ -66.91 after sell → book $9,792.78; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NIQ` | 64 | $19.00 | $2.20 | $+9.06 | $5,023.80 | ▲ +9.06 after sell → book $9,790.58; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERO` | 30 | $35.95 | $2.10 | $-101.68 | $6,100.20 | ▼ -101.68 after sell → book $9,788.48; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `CVI` | 30 | $42.94 | $2.10 | $+82.82 | $7,386.30 | ▲ +82.82 after sell → book $9,786.38; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `VIRT` | 18 | $65.38 | $2.06 | $-4.83 | $8,561.07 | ▼ -4.83 after sell → book $9,784.31; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,561.07 | ▲ 09:30 equity $9,809.75 vs yday $9,805.51 (+4.24) | 09:30 open · cash $8,561.07 (unchanged overnight, no fees) · equity $9,809.75 vs prior close $9,805.51 (+4.24) because holdings re-marked: TRLV×106 yday $11.74 → 09:30 $11.78 +4.24 | — |
| 2026-09-03 09:30 ET | **SELL** | `TRLV` | 106 | $11.78 | $2.34 | $+37.76 | $9,807.42 | ▲ +37.76 after sell → book $9,807.42; vs 09:30 mark -2.33 | dropped from list after 4 sess (min 3) | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $49.76 | $2.06 | — | $8,611.12 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1225.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $7,475.64 | — | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1225.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1829 | $0.67 | $17.74 | — | $6,232.47 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1225.93 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.46 | $2.21 | — | $5,012.21 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1225.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 1066 | $1.15 | $13.75 | — | $3,772.56 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+10.1; leftover $1225.93 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 119 | $10.27 | $2.35 | — | $2,548.09 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1225.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4086 | $0.30 | $24.52 | — | $1,297.77 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_mover; 🔵; ret5=+54.3; leftover $1225.93 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CDXS` | 806 | $1.52 | $10.40 | — | $62.25 | — | union ∩ break10, no 🚨; gate break_10=True; list yday_mover; ret5=+7.1; leftover $1225.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.25 | ▲ 09:30 equity $10,223.92 vs yday $9,865.32 (+358.60) | 09:30 open · cash $62.25 (unchanged overnight, no fees) · equity $10,223.92 vs prior close $9,865.32 (+358.60) because holdings re-marked: ATRC×24 yday $52.59 → 09:30 $52.88 +6.96; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; DEFT×1829 yday $0.65 → 09:30 $0.65 +0.00; ARCT×74 yday $16.74 → 09:30 $16.77 +2.22; SID×1066 yday $1.17 → 09:30 $1.36 +202.54; NVAX×119 yday $10.32 → 09:30 $10.41 +10.71; CAN×4086 yday $0.31 → 09:30 $0.34 +122.58; CDXS×806 yday $1.48 → 09:30 $1.48 +0.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 1 | $11.89 | $0.12 | — | $50.24 | — | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $12.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 2.44 < 1 share @ 57.61 |
| 2026-08-14 | `ADUR` | cash | leftover split 2.44 < 1 share @ 16.50 |
| 2026-08-14 | `ARX` | cash | leftover split 2.44 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 2.44 < 1 share @ 11.12 |
| 2026-08-14 | `TBBB` | cash | leftover split 2.44 < 1 share @ 48.82 |
| 2026-08-14 | `AMPY` | cash | leftover split 2.44 < 1 share @ 4.94 |
| 2026-08-14 | `SNDK` | cash | leftover split 2.44 < 1 share @ 1646.93 |
| 2026-08-14 | `MH` | cash | leftover split 2.44 < 1 share @ 13.55 |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 2.44 < 1 share @ 46.18 |
| 2026-08-17 | `OCC` | cash | leftover split 2.44 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 2.44 < 1 share @ 16.20 |
| 2026-08-17 | `CAPR` | cash | leftover split 2.44 < 1 share @ 6.87 |
| 2026-08-17 | `HTFL` | cash | leftover split 2.44 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 2.44 < 1 share @ 32.55 |
| 2026-08-17 | `LPTH` | cash | leftover split 2.44 < 1 share @ 14.94 |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALEC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 7.42 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 7.42 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 7.42 < 1 share @ 216.30 |
| 2026-08-21 | `TEM` | cash | leftover split 7.42 < 1 share @ 65.60 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ORBS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAN` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DFDV` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `NIQ` | no_price | no 09:30 open |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TRLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VIRT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DEFT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NIQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VIRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GUTS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `UEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SIBN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GUTS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CDXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HQ` | cash | leftover split 12.45 < 1 share @ 17.06 |
| 2026-09-04 | `NIQ` | cash | leftover split 12.45 < 1 share @ 18.66 |
| 2026-09-04 | `OMER` | cash | leftover split 12.45 < 1 share @ 18.99 |
| 2026-09-04 | `ERO` | cash | leftover split 12.45 < 1 share @ 35.82 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 24 | 2026-09-03 @ $49.76 | union ∩ break10, no 🚨; gate break_10=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1225.93 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | union ∩ break10, no 🚨; gate break_10=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1225.93 |
| `DEFT` | 1829 | 2026-09-03 @ $0.67 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1225.93 |
| `ARCT` | 74 | 2026-09-03 @ $16.46 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1225.93 |
| `SID` | 1066 | 2026-09-03 @ $1.15 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer; 🔵; ret5=+10.1; leftover $1225.93 |
| `NVAX` | 119 | 2026-09-03 @ $10.27 | union ∩ break10, no 🚨; gate break_10=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1225.93 |
| `CAN` | 4086 | 2026-09-03 @ $0.30 | union ∩ break10, no 🚨; gate break_10=True; list yday_mover; 🔵; ret5=+54.3; leftover $1225.93 |
| `CDXS` | 806 | 2026-09-03 @ $1.52 | union ∩ break10, no 🚨; gate break_10=True; list yday_mover; ret5=+7.1; leftover $1225.93 |
| `TRLV` | 1 | 2026-09-04 @ $11.89 | union ∩ break10, no 🚨; gate break_10=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $12.45 |
