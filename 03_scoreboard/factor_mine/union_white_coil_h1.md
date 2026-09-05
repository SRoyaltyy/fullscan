# Factor mine action — `union_white_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+2.05%** ($10,205) · signal-only (no cash/fees) was +7.42%. Starts YES **11/17**. Fills 124 · skips 8 · realized $+234.04.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `zero_red=True,ret_5_max=10.0,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $293.60.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, TPG, TGTX, SLS, HIMS, VOR | — | $134.73 | $10,069.06 | $10,203.79 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75 | BUY BTSG x27 @ 59.80; BUY TPG x32 @ 50.62; BUY TGTX x33 @ 49.70; BUY SLS x142 @ 11.70; BUY HIMS x56 @ 29.74; BUY VOR x75 @ 22.01 |
| 2026-08-14 | +5.50 | $134.73 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75 | DAVE, MARA, LDI, BTBT, BETR, ANGX, HYLN, WDC | BTSG, TPG, TGTX, SLS, HIMS, VOR | $520.49 | $9,582.06 | $10,102.54 | DAVE×3, MARA×141, LDI×1361, BTBT×850, BETR×86, ANGX×295, HYLN×305, WDC×2 | SELL BTSG (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); SELL TGTX (dropped from list after 1 sess (min 1)); SELL SLS (dropped from list after 1 sess (min 1)); SELL HIMS (dropped from list after 1 sess (min 1)); SELL VOR (dropped from list after 1 sess (min 1)); BUY DAVE x3 @ 330.91; BUY MARA x141 @ 9.01; BUY LDI x1361 @ 0.94; BUY BTBT x850 @ 1.50; BUY BETR x86 @ 14.80; BUY ANGX x295 @ 4.31; BUY HYLN x305 @ 4.18; BUY WDC x2 @ 503.50 |
| 2026-08-17 | +2.25 | $520.49 | DAVE×3, MARA×141, LDI×1361, BTBT×850, BETR×86, ANGX×295, HYLN×305, WDC×2 | TMC, TGB, DNN, CDNL, ABX, OCC, ALM, MRLN | DAVE, MARA, LDI, BTBT, BETR, ANGX, HYLN, WDC | $35.03 | $9,870.03 | $9,905.06 | TMC×313, TGB×149, DNN×391, CDNL×31, ABX×139, OCC×69, ALM×78, MRLN×338 | SELL DAVE (dropped from list after 1 sess (min 1)); SELL MARA (dropped from list after 1 sess (min 1)); SELL LDI (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); SELL BETR (dropped from list after 1 sess (min 1)); SELL ANGX (dropped from list after 1 sess (min 1)); SELL HYLN (dropped from list after 1 sess (min 1)); SELL WDC (dropped from list after 1 sess (min 1)); BUY TMC x313 @ 4.05; BUY TGB x149 @ 8.46; BUY DNN x391 @ 3.24; BUY CDNL x31 @ 39.85; BUY ABX x139 @ 9.12; BUY OCC x69 @ 18.24; BUY ALM x78 @ 16.20; BUY MRLN x338 @ 3.75 |
| 2026-08-18 | -6.20 | $35.03 | TMC×313, TGB×149, DNN×391, CDNL×31, ABX×139, OCC×69, ALM×78, MRLN×338 | — | TMC, TGB, DNN, CDNL, ABX, OCC, ALM, MRLN | $9,739.70 | $0.00 | $9,739.70 | — | SELL TMC (dropped from list after 1 sess (min 1)); SELL TGB (dropped from list after 1 sess (min 1)); SELL DNN (dropped from list after 1 sess (min 1)); SELL CDNL (dropped from list after 1 sess (min 1)); SELL ABX (dropped from list after 1 sess (min 1)); SELL OCC (dropped from list after 1 sess (min 1)); SELL ALM (dropped from list after 1 sess (min 1)); SELL MRLN (dropped from list after 1 sess (min 1)) |
| 2026-08-19 | -7.20 | $9,739.70 | — | — | — | $9,739.70 | $0.00 | $9,739.70 | — | hard-red sit S=-7.20 |
| 2026-08-20 | +1.12 | $9,739.70 | — | AG, BHP, HDSN, IAG, KGC, NFGC, MRVI, SCZM | — | $38.07 | $10,016.90 | $10,054.97 | AG×59, BHP×13, HDSN×210, IAG×62, KGC×41, NFGC×695, MRVI×164, SCZM×128 | BUY AG x59 @ 20.55; BUY BHP x13 @ 91.01; BUY HDSN x210 @ 5.77; BUY IAG x62 @ 19.63; BUY KGC x41 @ 29.63; BUY NFGC x695 @ 1.75; BUY MRVI x164 @ 7.38; BUY SCZM x128 @ 9.46 |
| 2026-08-21 | +3.25 | $38.07 | AG×59, BHP×13, HDSN×210, IAG×62, KGC×41, NFGC×695, MRVI×164, SCZM×128 | CRSP, EMBC, TXG, BEKE, HITI | AG, BHP, HDSN, IAG, KGC, NFGC, SCZM | $98.04 | $10,183.98 | $10,282.02 | MRVI×164, CRSP×29, EMBC×328, TXG×27, BEKE×99, HITI×735 | SELL AG (dropped from list after 1 sess (min 1)); SELL BHP (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL SCZM (dropped from list after 1 sess (min 1)); BUY CRSP x29 @ 59.72; BUY EMBC x328 @ 5.43; BUY TXG x27 @ 64.39; BUY BEKE x99 @ 17.93; BUY HITI x735 @ 2.43 |
| 2026-08-24 | -5.17 | $98.04 | MRVI×164, CRSP×29, EMBC×328, TXG×27, BEKE×99, HITI×735 | — | MRVI, CRSP, EMBC, TXG, BEKE, HITI | $10,189.22 | $0.00 | $10,189.22 | — | SELL MRVI (dropped from list after 2 sess (min 1)); SELL CRSP (dropped from list after 1 sess (min 1)); SELL EMBC (dropped from list after 1 sess (min 1)); SELL TXG (dropped from list after 1 sess (min 1)); SELL BEKE (dropped from list after 1 sess (min 1)); SELL HITI (dropped from list after 1 sess (min 1)) |
| 2026-08-25 | +1.80 | $10,189.22 | — | CRMD, BMEA, ZURA, EZPW, BZ, VIPS, RHI, SUZ | — | $56.51 | $10,218.37 | $10,274.88 | CRMD×153, BMEA×786, ZURA×199, EZPW×36, BZ×83, VIPS×91, RHI×28, SUZ×140 | BUY CRMD x153 @ 8.28; BUY BMEA x786 @ 1.62; BUY ZURA x199 @ 6.38; BUY EZPW x36 @ 34.48; BUY BZ x83 @ 15.34; BUY VIPS x91 @ 13.91; BUY RHI x28 @ 44.52; BUY SUZ x140 @ 9.07 |
| 2026-08-26 | +2.02 | $56.51 | CRMD×153, BMEA×786, ZURA×199, EZPW×36, BZ×83, VIPS×91, RHI×28, SUZ×140 | — | — | $56.51 | $10,106.45 | $10,162.96 | CRMD×153, BMEA×786, ZURA×199, EZPW×36, BZ×83, VIPS×91, RHI×28, SUZ×140 | hold CRMD,BMEA,ZURA,EZPW,BZ,VIPS,RHI,SUZ |
| 2026-08-27 | — | $56.51 | CRMD×153, BMEA×786, ZURA×199, EZPW×36, BZ×83, VIPS×91, RHI×28, SUZ×140 | — | CRMD, BMEA, ZURA, EZPW, BZ, VIPS, RHI, SUZ | $10,397.63 | $0.00 | $10,397.63 | — | SELL CRMD (dropped from list after 2 sess (min 1)); SELL BMEA (dropped from list after 2 sess (min 1)); SELL ZURA (dropped from list after 2 sess (min 1)); SELL EZPW (dropped from list after 2 sess (min 1)); SELL BZ (dropped from list after 2 sess (min 1)); SELL VIPS (dropped from list after 2 sess (min 1)); SELL RHI (dropped from list after 2 sess (min 1)); SELL SUZ (dropped from list after 2 sess (min 1)) |
| 2026-08-28 | +0.75 | $10,397.63 | — | SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC, MEI | — | $363.57 | $9,950.32 | $10,313.89 | SMTC×8, TTMI×10, KEYS×4, AVT×14, CGNX×20, COHR×4, LSCC×10, MEI×75 | BUY SMTC x8 @ 149.40; BUY TTMI x10 @ 127.07; BUY KEYS x4 @ 323.82; BUY AVT x14 @ 91.11; BUY CGNX x20 @ 62.80; BUY COHR x4 @ 303.67; BUY LSCC x10 @ 121.13; BUY MEI x75 @ 17.32 |
| 2026-08-31 | -5.85 | $363.57 | SMTC×8, TTMI×10, KEYS×4, AVT×14, CGNX×20, COHR×4, LSCC×10, MEI×75 | — | SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC, MEI | $9,949.22 | $0.00 | $9,949.22 | — | SELL SMTC (dropped from list after 1 sess (min 1)); SELL TTMI (dropped from list after 1 sess (min 1)); SELL KEYS (dropped from list after 1 sess (min 1)); SELL AVT (dropped from list after 1 sess (min 1)); SELL CGNX (dropped from list after 1 sess (min 1)); SELL COHR (dropped from list after 1 sess (min 1)); SELL LSCC (dropped from list after 1 sess (min 1)); SELL MEI (dropped from list after 1 sess (min 1)) |
| 2026-09-01 | -6.30 | $9,949.22 | — | — | — | $9,949.22 | $0.00 | $9,949.22 | — | hard-red sit S=-6.30 |
| 2026-09-02 | -3.83 | $9,949.22 | — | — | — | $9,949.22 | $0.00 | $9,949.22 | — | hard-red sit S=-3.83 |
| 2026-09-03 | -0.90 | $9,949.22 | — | HRMY, VSTM, RVTY, MMED, CRDL, BMEA, VIR, NEOV | — | $117.40 | $10,118.39 | $10,235.79 | HRMY×30, VSTM×161, RVTY×9, MMED×54, CRDL×575, BMEA×690, VIR×106, NEOV×339 | BUY HRMY x30 @ 41.31; BUY VSTM x161 @ 7.70; BUY RVTY x9 @ 125.94; BUY MMED x54 @ 22.78; BUY CRDL x575 @ 2.16; BUY BMEA x690 @ 1.80; BUY VIR x106 @ 11.63; BUY NEOV x339 @ 3.66 |
| 2026-09-04 | — | $117.40 | HRMY×30, VSTM×161, RVTY×9, MMED×54, CRDL×575, BMEA×690, VIR×106, NEOV×339 | BVS, DELL, MLYS, TARS, LENZ, INO | HRMY, VSTM, RVTY, MMED, CRDL, BMEA, VIR, NEOV | $293.60 | $9,911.22 | $10,204.82 | BVS×117, DELL×3, MLYS×58, TARS×20, LENZ×289, INO×1245 | SELL HRMY (dropped from list after 1 sess (min 1)); SELL VSTM (dropped from list after 1 sess (min 1)); SELL RVTY (dropped from list after 1 sess (min 1)); SELL MMED (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL BMEA (dropped from list after 1 sess (min 1)); SELL VIR (dropped from list after 1 sess (min 1)); SELL NEOV (dropped from list after 1 sess (min 1)); BUY BVS x117 @ 14.50; BUY DELL x3 @ 486.31; BUY MLYS x58 @ 29.15; BUY TARS x20 @ 82.76; BUY LENZ x289 @ 5.90; BUY INO x1245 @ 1.37 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 27 | $59.80 | $2.07 | — | $8,383.33 | ▼ $9,997.93 (-2.07) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $6,761.30 | ▼ $9,995.84 (-4.16) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 33 | $49.70 | $2.09 | — | $5,119.11 | ▼ $9,993.75 (-6.25) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 142 | $11.70 | $2.42 | — | $3,455.30 | ▼ $9,991.34 (-8.66) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 56 | $29.74 | $2.16 | — | $1,787.70 | ▼ $9,989.18 (-10.82) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 75 | $22.01 | $2.21 | — | $134.73 | ▼ $9,986.96 (-13.04) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 27 | $59.65 | $2.09 | $-8.21 | $1,743.19 | ▲ $10,215.33 (+215.33) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $3,510.36 | ▲ $10,213.22 (+213.22) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 33 | $47.27 | $2.11 | $-84.39 | $5,068.16 | ▲ $10,211.11 (+211.11) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 142 | $12.40 | $2.45 | $+94.53 | $6,826.50 | ▲ $10,208.65 (+208.65) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 56 | $29.15 | $2.18 | $-37.38 | $8,456.72 | ▲ $10,206.47 (+206.47) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 75 | $23.33 | $2.24 | $+94.54 | $10,204.23 | ▲ $10,204.23 (+204.23) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,209.50 | ▲ $10,202.23 (+202.23) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 141 | $9.01 | $2.41 | — | $7,936.68 | ▲ $10,199.82 (+199.82) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1361 | $0.94 | $16.84 | — | $6,644.59 | ▲ $10,182.98 (+182.98) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 850 | $1.50 | $10.96 | — | $5,358.62 | ▲ $10,172.02 (+172.02) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 86 | $14.80 | $2.25 | — | $4,083.57 | ▲ $10,169.77 (+169.77) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 295 | $4.31 | $3.81 | — | $2,808.32 | ▲ $10,165.96 (+165.96) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 305 | $4.18 | $3.93 | — | $1,529.48 | ▲ $10,162.03 (+162.03) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $520.49 | ▲ $10,160.03 (+160.03) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $1,529.29 | ▲ $10,189.92 (+189.92) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 141 | $9.22 | $2.45 | $+24.75 | $2,826.86 | ▲ $10,187.47 (+187.47) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1361 | $0.91 | $16.66 | $-74.33 | $4,044.63 | ▲ $10,170.81 (+170.81) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 850 | $1.52 | $11.12 | $-5.08 | $5,325.51 | ▲ $10,159.69 (+159.69) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 86 | $13.67 | $2.27 | $-101.70 | $6,498.86 | ▲ $10,157.42 (+157.42) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 295 | $4.60 | $3.87 | $+77.88 | $7,851.99 | ▲ $10,153.55 (+153.55) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 305 | $4.10 | $4.00 | $-32.33 | $9,098.50 | ▲ $10,149.56 (+149.56) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $10,147.54 | ▲ $10,147.54 (+147.54) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $8,875.85 | ▲ $10,143.50 (+143.50) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 149 | $8.46 | $2.44 | — | $7,612.88 | ▲ $10,141.07 (+141.07) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $6,340.99 | ▲ $10,136.02 (+136.02) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $5,103.56 | ▲ $10,133.94 (+133.94) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1268.44 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 139 | $9.12 | $2.41 | — | $3,833.47 | ▲ $10,131.53 (+131.53) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 69 | $18.24 | $2.20 | — | $2,572.71 | ▲ $10,129.33 (+129.33) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1268.44 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 78 | $16.20 | $2.22 | — | $1,306.89 | ▲ $10,127.11 (+127.11) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MRLN` | 338 | $3.75 | $4.36 | — | $35.03 | ▲ $10,122.75 (+122.75) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-15.4; leftover $1268.44 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $1,195.29 | ▼ $9,760.73 (-239.27) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 149 | $8.55 | $2.47 | $+8.50 | $2,466.77 | ▼ $9,758.26 (-241.74) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 391 | $3.11 | $5.12 | $-60.99 | $3,677.66 | ▼ $9,753.14 (-246.86) | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $4,964.23 | ▼ $9,751.04 (-248.96) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 139 | $9.03 | $2.44 | $-17.36 | $6,216.96 | ▼ $9,748.60 (-251.40) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 69 | $16.20 | $2.22 | $-145.18 | $7,332.54 | ▼ $9,746.38 (-253.62) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 78 | $15.78 | $2.25 | $-37.23 | $8,561.13 | ▼ $9,744.13 (-255.87) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `MRLN` | 338 | $3.50 | $4.43 | $-93.29 | $9,739.70 | ▼ $9,739.70 (-260.30) | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,525.09 | ▼ $9,737.54 (-262.46) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,339.93 | ▼ $9,735.51 (-264.49) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 210 | $5.77 | $2.71 | — | $6,125.52 | ▼ $9,732.80 (-267.20) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 62 | $19.63 | $2.18 | — | $4,906.28 | ▼ $9,730.62 (-269.38) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $3,689.34 | ▼ $9,728.51 (-271.49) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 695 | $1.75 | $8.97 | — | $2,464.13 | ▼ $9,719.55 (-280.45) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 164 | $7.38 | $2.48 | — | $1,251.32 | ▼ $9,717.06 (-282.94) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 128 | $9.46 | $2.37 | — | $38.07 | ▼ $9,714.69 (-285.31) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 59 | $21.90 | $2.19 | $+75.30 | $1,327.98 | ▲ $10,296.68 (+296.68) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,570.29 | ▲ $10,294.63 (+294.63) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 210 | $5.67 | $2.75 | $-26.46 | $3,758.24 | ▲ $10,291.88 (+291.88) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 62 | $21.17 | $2.20 | $+91.11 | $5,068.58 | ▲ $10,289.68 (+289.68) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $6,385.42 | ▲ $10,287.55 (+287.55) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 695 | $1.79 | $9.09 | $+9.74 | $7,620.38 | ▲ $10,278.46 (+278.46) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 128 | $10.26 | $2.41 | $+97.62 | $8,931.25 | ▲ $10,276.05 (+276.05) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 29 | $59.72 | $2.08 | — | $7,197.30 | ▲ $10,273.98 (+273.98) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1786.25 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 328 | $5.43 | $4.23 | — | $5,412.02 | ▲ $10,269.74 (+269.74) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1786.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 27 | $64.39 | $2.07 | — | $3,671.42 | ▲ $10,267.67 (+267.67) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1786.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 99 | $17.93 | $2.29 | — | $1,893.57 | ▲ $10,265.39 (+265.39) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1786.25 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 735 | $2.43 | $9.48 | — | $98.04 | ▲ $10,255.90 (+255.90) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $1786.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 164 | $8.59 | $2.52 | $+193.44 | $1,504.28 | ▲ $10,209.65 (+209.65) | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 29 | $58.79 | $2.10 | $-31.15 | $3,207.09 | ▲ $10,207.55 (+207.55) | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 328 | $5.21 | $4.30 | $-80.69 | $4,911.67 | ▲ $10,203.25 (+203.25) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 27 | $63.07 | $2.09 | $-39.81 | $6,612.46 | ▲ $10,201.15 (+201.15) | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 99 | $18.06 | $2.32 | $+7.77 | $8,398.09 | ▲ $10,198.84 (+198.84) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 735 | $2.45 | $9.62 | $-4.40 | $10,189.22 | ▲ $10,189.22 (+189.22) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 153 | $8.28 | $2.45 | — | $8,919.93 | ▲ $10,186.77 (+186.77) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1273.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 786 | $1.62 | $10.14 | — | $7,636.47 | ▲ $10,176.63 (+176.63) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1273.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 199 | $6.38 | $2.59 | — | $6,364.26 | ▲ $10,174.04 (+174.04) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1273.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 36 | $34.48 | $2.10 | — | $5,120.89 | ▲ $10,171.95 (+171.95) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1273.65 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 83 | $15.34 | $2.24 | — | $3,845.43 | ▲ $10,169.71 (+169.71) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1273.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 91 | $13.91 | $2.26 | — | $2,577.35 | ▲ $10,167.44 (+167.44) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.5; leftover $1273.65 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 28 | $44.52 | $2.07 | — | $1,328.72 | ▲ $10,165.37 (+165.37) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+3.5; leftover $1273.65 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 140 | $9.07 | $2.41 | — | $56.51 | ▲ $10,162.96 (+162.96) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; ⚪; ret5=+8.3; leftover $1273.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 153 | $8.60 | $2.48 | $+44.03 | $1,369.82 | ▲ $10,421.74 (+421.74) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 786 | $1.75 | $10.28 | $+81.76 | $2,735.04 | ▲ $10,411.46 (+411.46) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 199 | $6.13 | $2.63 | $-54.97 | $3,952.28 | ▲ $10,408.83 (+408.83) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EZPW` | 36 | $35.70 | $2.12 | $+39.70 | $5,235.37 | ▲ $10,406.72 (+406.72) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 83 | $16.77 | $2.26 | $+114.19 | $6,625.01 | ▲ $10,404.45 (+404.45) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIPS` | 91 | $14.00 | $2.29 | $+3.64 | $7,896.72 | ▲ $10,402.16 (+402.16) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RHI` | 28 | $44.33 | $2.09 | $-9.49 | $9,135.87 | ▲ $10,400.07 (+400.07) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUZ` | 140 | $9.03 | $2.44 | $-10.45 | $10,397.63 | ▲ $10,397.63 (+397.63) | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $9,200.41 | ▲ $10,395.61 (+395.61) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $127.07 | $2.02 | — | $7,927.69 | ▲ $10,393.59 (+393.59) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $323.82 | $2.00 | — | $6,630.41 | ▲ $10,391.59 (+391.59) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-11.7; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.11 | $2.03 | — | $5,352.84 | ▲ $10,389.56 (+389.56) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-7.4; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.80 | $2.05 | — | $4,094.79 | ▲ $10,387.51 (+387.51) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-7.8; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $303.67 | $2.00 | — | $2,878.11 | ▲ $10,385.51 (+385.51) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-11.1; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $121.13 | $2.02 | — | $1,664.79 | ▲ $10,383.49 (+383.49) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-9.8; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 75 | $17.32 | $2.21 | — | $363.57 | ▲ $10,381.27 (+381.27) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-16.7; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $1,425.86 | ▼ $9,963.71 (-36.29) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $117.20 | $2.04 | $-102.76 | $2,595.82 | ▼ $9,961.67 (-38.33) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $324.14 | $2.02 | $-2.74 | $3,890.35 | ▼ $9,959.64 (-40.36) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $88.63 | $2.05 | $-38.80 | $5,129.12 | ▼ $9,957.59 (-42.41) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.31 | $2.07 | $-53.92 | $6,333.25 | ▼ $9,955.52 (-44.48) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $274.13 | $2.02 | $-122.18 | $7,427.75 | ▼ $9,953.50 (-46.50) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 10 | $116.00 | $2.04 | $-55.36 | $8,585.71 | ▼ $9,951.46 (-48.54) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MEI` | 75 | $18.21 | $2.24 | $+62.30 | $9,949.22 | ▼ $9,949.22 (-50.78) | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $8,707.84 | ▼ $9,947.14 (-52.86) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 161 | $7.70 | $2.47 | — | $7,465.67 | ▼ $9,944.67 (-55.33) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $6,330.19 | ▼ $9,942.65 (-57.35) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 54 | $22.78 | $2.15 | — | $5,097.92 | ▼ $9,940.50 (-59.50) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 575 | $2.16 | $7.42 | — | $3,848.50 | ▼ $9,933.08 (-66.92) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 690 | $1.80 | $8.90 | — | $2,597.60 | ▼ $9,924.18 (-75.82) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 106 | $11.63 | $2.31 | — | $1,362.51 | ▼ $9,921.87 (-78.13) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.8; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NEOV` | 339 | $3.66 | $4.37 | — | $117.40 | ▼ $9,917.50 (-82.50) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; 🔵; ⚪; ret5=-8.0; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 30 | $42.93 | $2.10 | $+44.42 | $1,403.20 | ▲ $10,264.07 (+264.07) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 161 | $8.03 | $2.51 | $+48.15 | $2,693.52 | ▲ $10,261.56 (+261.56) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $3,883.53 | ▲ $10,259.52 (+259.52) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 54 | $23.88 | $2.17 | $+55.08 | $5,170.88 | ▲ $10,257.35 (+257.35) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 575 | $2.18 | $7.52 | $-3.44 | $6,416.86 | ▲ $10,249.83 (+249.83) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 690 | $1.93 | $9.03 | $+71.77 | $7,739.53 | ▲ $10,240.80 (+240.80) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VIR` | 106 | $11.54 | $2.34 | $-14.18 | $8,960.44 | ▲ $10,238.47 (+238.47) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NEOV` | 339 | $3.77 | $4.44 | $+28.48 | $10,234.03 | ▲ $10,234.03 (+234.03) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 117 | $14.50 | $2.34 | — | $8,535.19 | ▲ $10,231.69 (+231.69) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1705.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $7,074.26 | ▲ $10,229.69 (+229.69) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1705.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 58 | $29.15 | $2.16 | — | $5,381.39 | ▲ $10,227.52 (+227.52) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1705.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 20 | $82.76 | $2.05 | — | $3,724.14 | ▲ $10,225.47 (+225.47) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1705.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 289 | $5.90 | $3.73 | — | $2,015.31 | ▲ $10,221.74 (+221.74) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=-1.1; leftover $1705.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `INO` | 1245 | $1.37 | $16.06 | — | $293.60 | ▲ $10,205.68 (+205.68) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $1705.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-26 | `CRMD` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `EZPW` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BZ` | no_price | no 09:30 open — carry |
| 2026-08-26 | `VIPS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RHI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SUZ` | no_price | no 09:30 open — carry |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BVS` | 117 | 2026-09-04 @ $14.50 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1705.67 |
| `DELL` | 3 | 2026-09-04 @ $486.31 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1705.67 |
| `MLYS` | 58 | 2026-09-04 @ $29.15 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1705.67 |
| `TARS` | 20 | 2026-09-04 @ $82.76 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1705.67 |
| `LENZ` | 289 | 2026-09-04 @ $5.90 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=-1.1; leftover $1705.67 |
| `INO` | 1245 | 2026-09-04 @ $1.37 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $1705.67 |
