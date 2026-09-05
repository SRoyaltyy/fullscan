# Factor mine action — `union_white_coil_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+4.51%** ($10,451) · signal-only (no cash/fees) was +1.34%. Starts YES **8/17**. Fills 94 · skips 107 · realized $+201.75.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `zero_red=True,ret_5_max=10.0,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $42.93.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, TPG, TGTX, SLS, HIMS, VOR | — | $134.73 | $10,069.06 | $10,203.79 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75 | BUY BTSG x27 @ 59.80; BUY TPG x32 @ 50.62; BUY TGTX x33 @ 49.70; BUY SLS x142 @ 11.70; BUY HIMS x56 @ 29.74; BUY VOR x75 @ 22.01 |
| 2026-08-14 | +5.50 | $134.73 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75 | MARA, LDI, BTBT, BETR, ANGX, HYLN | — | $47.87 | $10,174.76 | $10,222.63 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75, MARA×1, LDI×17, BTBT×11, BETR×1, ANGX×3, HYLN×4 | BUY MARA x1 @ 9.01; BUY LDI x17 @ 0.94; BUY BTBT x11 @ 1.50; BUY BETR x1 @ 14.80; BUY ANGX x3 @ 4.31; BUY HYLN x4 @ 4.18 |
| 2026-08-17 | +2.25 | $47.87 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75, MARA×1, LDI×17, BTBT×11, BETR×1, ANGX×3, HYLN×4 | TMC, DNN, MRLN | — | $36.71 | $10,183.77 | $10,220.48 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75, MARA×1, LDI×17, BTBT×11, BETR×1, ANGX×3, HYLN×4, TMC×1, DNN×1, MRLN×1 | BUY TMC x1 @ 4.05; BUY DNN x1 @ 3.24; BUY MRLN x1 @ 3.75 |
| 2026-08-18 | -6.20 | $36.71 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75, MARA×1, LDI×17, BTBT×11, BETR×1, ANGX×3, HYLN×4, TMC×1, DNN×1, MRLN×1 | — | BTSG, TPG, TGTX, SLS, HIMS, VOR | $9,995.22 | $92.92 | $10,088.14 | MARA×1, LDI×17, BTBT×11, BETR×1, ANGX×3, HYLN×4, TMC×1, DNN×1, MRLN×1 | SELL BTSG (dropped from list after 3 sess (min 3)); SELL TPG (dropped from list after 3 sess (min 3)); SELL TGTX (dropped from list after 3 sess (min 3)); SELL SLS (dropped from list after 3 sess (min 3)); SELL HIMS (dropped from list after 3 sess (min 3)); SELL VOR (dropped from list after 3 sess (min 3)) |
| 2026-08-19 | -7.20 | $9,995.22 | MARA×1, LDI×17, BTBT×11, BETR×1, ANGX×3, HYLN×4, TMC×1, DNN×1, MRLN×1 | — | MARA, LDI, BTBT, BETR, ANGX, HYLN | $10,076.54 | $10.51 | $10,087.04 | TMC×1, DNN×1, MRLN×1 | SELL MARA (dropped from list after 3 sess (min 3)); SELL LDI (dropped from list after 3 sess (min 3)); SELL BTBT (dropped from list after 3 sess (min 3)); SELL BETR (dropped from list after 3 sess (min 3)); SELL ANGX (dropped from list after 3 sess (min 3)); SELL HYLN (dropped from list after 3 sess (min 3)) |
| 2026-08-20 | +1.12 | $10,076.54 | TMC×1, DNN×1, MRLN×1 | AG, BHP, HDSN, IAG, KGC, NFGC, MRVI, SCZM | TMC, DNN, MRLN | $93.20 | $10,318.38 | $10,411.58 | AG×61, BHP×13, HDSN×218, IAG×64, KGC×42, NFGC×720, MRVI×170, SCZM×133 | SELL TMC (dropped from list after 3 sess (min 3)); SELL DNN (dropped from list after 3 sess (min 3)); SELL MRLN (dropped from list after 3 sess (min 3)); BUY AG x61 @ 20.55; BUY BHP x13 @ 91.01; BUY HDSN x218 @ 5.77; BUY IAG x64 @ 19.63; BUY KGC x42 @ 29.63; BUY NFGC x720 @ 1.75; BUY MRVI x170 @ 7.38; BUY SCZM x133 @ 9.46 |
| 2026-08-21 | +3.25 | $93.20 | AG×61, BHP×13, HDSN×218, IAG×64, KGC×42, NFGC×720, MRVI×170, SCZM×133 | EMBC, BEKE, HITI | — | $41.42 | $10,645.26 | $10,686.68 | AG×61, BHP×13, HDSN×218, IAG×64, KGC×42, NFGC×720, MRVI×170, SCZM×133, EMBC×3, BEKE×1, HITI×7 | BUY EMBC x3 @ 5.43; BUY BEKE x1 @ 17.93; BUY HITI x7 @ 2.43 |
| 2026-08-24 | -5.17 | $41.42 | AG×61, BHP×13, HDSN×218, IAG×64, KGC×42, NFGC×720, MRVI×170, SCZM×133, EMBC×3, BEKE×1, HITI×7 | — | — | $41.42 | $10,546.37 | $10,587.79 | AG×61, BHP×13, HDSN×218, IAG×64, KGC×42, NFGC×720, MRVI×170, SCZM×133, EMBC×3, BEKE×1, HITI×7 | hard-red sit S=-5.17 |
| 2026-08-25 | +1.80 | $41.42 | AG×61, BHP×13, HDSN×218, IAG×64, KGC×42, NFGC×720, MRVI×170, SCZM×133, EMBC×3, BEKE×1, HITI×7 | CRMD, BMEA, ZURA, EZPW, BZ, VIPS, RHI, SUZ | AG, BHP, HDSN, IAG, KGC, NFGC, MRVI, SCZM | $34.48 | $10,659.22 | $10,693.70 | EMBC×3, BEKE×1, HITI×7, CRMD×159, BMEA×814, ZURA×206, EZPW×38, BZ×86, VIPS×94, RHI×29, SUZ×145 | SELL AG (dropped from list after 3 sess (min 3)); SELL BHP (dropped from list after 3 sess (min 3)); SELL HDSN (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); SELL KGC (dropped from list after 3 sess (min 3)); SELL NFGC (dropped from list after 3 sess (min 3)); SELL MRVI (dropped from list after 3 sess (min 3)); SELL SCZM (dropped from list after 3 sess (min 3)); BUY CRMD x159 @ 8.28; BUY BMEA x814 @ 1.62; BUY ZURA x206 @ 6.38; BUY EZPW x38 @ 34.48; BUY BZ x86 @ 15.34; BUY VIPS x94 @ 13.91; BUY RHI x29 @ 44.52; BUY SUZ x145 @ 9.07 |
| 2026-08-26 | +2.02 | $34.48 | EMBC×3, BEKE×1, HITI×7, CRMD×159, BMEA×814, ZURA×206, EZPW×38, BZ×86, VIPS×94, RHI×29, SUZ×145 | — | — | $34.48 | $10,542.45 | $10,576.93 | EMBC×3, BEKE×1, HITI×7, CRMD×159, BMEA×814, ZURA×206, EZPW×38, BZ×86, VIPS×94, RHI×29, SUZ×145 | hold EMBC,BEKE,HITI,CRMD,BMEA,ZURA,EZPW,BZ,VIPS,RHI,SUZ |
| 2026-08-27 | — | $34.48 | EMBC×3, BEKE×1, HITI×7, CRMD×159, BMEA×814, ZURA×206, EZPW×38, BZ×86, VIPS×94, RHI×29, SUZ×145 | — | EMBC, BEKE, HITI | $84.95 | $10,779.81 | $10,864.76 | CRMD×159, BMEA×814, ZURA×206, EZPW×38, BZ×86, VIPS×94, RHI×29, SUZ×145 | SELL EMBC (dropped from list after 4 sess (min 3)); SELL BEKE (dropped from list after 4 sess (min 3)); SELL HITI (dropped from list after 4 sess (min 3)) |
| 2026-08-28 | +0.75 | $84.95 | CRMD×159, BMEA×814, ZURA×206, EZPW×38, BZ×86, VIPS×94, RHI×29, SUZ×145 | SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC, MEI | CRMD, BMEA, ZURA, EZPW, BZ, VIPS, RHI, SUZ | $400.36 | $10,329.53 | $10,729.89 | SMTC×9, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×78 | SELL CRMD (dropped from list after 3 sess (min 3)); SELL BMEA (dropped from list after 3 sess (min 3)); SELL ZURA (dropped from list after 3 sess (min 3)); SELL EZPW (dropped from list after 3 sess (min 3)); SELL BZ (dropped from list after 3 sess (min 3)); SELL VIPS (dropped from list after 3 sess (min 3)); SELL RHI (dropped from list after 3 sess (min 3)); SELL SUZ (dropped from list after 3 sess (min 3)); BUY SMTC x9 @ 149.40; BUY TTMI x10 @ 127.07; BUY KEYS x4 @ 323.82; BUY AVT x14 @ 91.11; BUY CGNX x21 @ 62.80; BUY COHR x4 @ 303.67; BUY LSCC x11 @ 121.13; BUY MEI x78 @ 17.32 |
| 2026-08-31 | -5.85 | $400.36 | SMTC×9, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×78 | — | — | $400.36 | $9,984.63 | $10,384.99 | SMTC×9, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×78 | hard-red sit S=-5.85 |
| 2026-09-01 | -6.30 | $400.36 | SMTC×9, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×78 | — | — | $400.36 | $9,920.16 | $10,320.52 | SMTC×9, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×78 | hard-red sit S=-6.30 |
| 2026-09-02 | -3.83 | $400.36 | SMTC×9, TTMI×10, KEYS×4, AVT×14, CGNX×21, COHR×4, LSCC×11, MEI×78 | — | SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC, MEI | $10,201.75 | $0.00 | $10,201.75 | — | SELL SMTC (dropped from list after 3 sess (min 3)); SELL TTMI (dropped from list after 3 sess (min 3)); SELL KEYS (dropped from list after 3 sess (min 3)); SELL AVT (dropped from list after 3 sess (min 3)); SELL CGNX (dropped from list after 3 sess (min 3)); SELL COHR (dropped from list after 3 sess (min 3)); SELL LSCC (dropped from list after 3 sess (min 3)); SELL MEI (dropped from list after 3 sess (min 3)) |
| 2026-09-03 | -0.90 | $10,201.75 | — | HRMY, VSTM, RVTY, MMED, CRDL, BMEA, VIR, NEOV | — | $57.21 | $10,440.98 | $10,498.19 | HRMY×30, VSTM×165, RVTY×10, MMED×55, CRDL×590, BMEA×708, VIR×109, NEOV×348 | BUY HRMY x30 @ 41.31; BUY VSTM x165 @ 7.70; BUY RVTY x10 @ 125.94; BUY MMED x55 @ 22.78; BUY CRDL x590 @ 2.16; BUY BMEA x708 @ 1.80; BUY VIR x109 @ 11.63; BUY NEOV x348 @ 3.66 |
| 2026-09-04 | — | $57.21 | HRMY×30, VSTM×165, RVTY×10, MMED×55, CRDL×590, BMEA×708, VIR×109, NEOV×348 | LENZ, INO | — | $42.93 | $10,408.08 | $10,451.01 | HRMY×30, VSTM×165, RVTY×10, MMED×55, CRDL×590, BMEA×708, VIR×109, NEOV×348, LENZ×1, INO×6 | BUY LENZ x1 @ 5.90; BUY INO x6 @ 1.37 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 27 | $59.80 | $2.07 | — | $8,383.33 | ▼ $9,997.93 (-2.07) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $6,761.30 | ▼ $9,995.84 (-4.16) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 33 | $49.70 | $2.09 | — | $5,119.11 | ▼ $9,993.75 (-6.25) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 142 | $11.70 | $2.42 | — | $3,455.30 | ▼ $9,991.34 (-8.66) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 56 | $29.74 | $2.16 | — | $1,787.70 | ▼ $9,989.18 (-10.82) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 75 | $22.01 | $2.21 | — | $134.73 | ▼ $9,986.96 (-13.04) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $125.63 | ▲ $10,217.33 (+217.33) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-13.5; leftover $16.84 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 17 | $0.94 | $0.21 | — | $109.49 | ▲ $10,217.12 (+217.12) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $16.84 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 11 | $1.50 | $0.20 | — | $92.79 | ▲ $10,216.92 (+216.92) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $16.84 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 1 | $14.80 | $0.15 | — | $77.84 | ▲ $10,216.77 (+216.77) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-9.9; leftover $16.84 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 3 | $4.31 | $0.14 | — | $64.77 | ▲ $10,216.63 (+216.63) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $16.84 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 4 | $4.18 | $0.18 | — | $47.87 | ▲ $10,216.45 (+216.45) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $16.84 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $43.78 | ▲ $10,201.40 (+201.40) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-12.3; leftover $5.98 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $40.50 | ▲ $10,201.36 (+201.36) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $5.98 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MRLN` | 1 | $3.75 | $0.04 | — | $36.71 | ▲ $10,201.32 (+201.32) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-15.4; leftover $5.98 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 27 | $60.00 | $2.09 | $+1.24 | $1,654.62 | ▲ $10,101.12 (+101.12) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 32 | $51.77 | $2.11 | $+32.50 | $3,309.15 | ▲ $10,099.01 (+99.01) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 33 | $49.28 | $2.11 | $-18.06 | $4,933.28 | ▲ $10,096.90 (+96.90) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 142 | $12.66 | $2.45 | $+131.45 | $6,728.55 | ▲ $10,094.44 (+94.44) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 56 | $27.85 | $2.18 | $-110.18 | $8,285.96 | ▲ $10,092.26 (+92.26) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 75 | $22.82 | $2.24 | $+56.29 | $9,995.22 | ▲ $10,090.02 (+90.02) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,004.02 | ▲ $10,088.00 (+88.00) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 17 | $0.88 | $0.22 | $-1.40 | $10,018.76 | ▲ $10,087.78 (+87.78) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 11 | $1.42 | $0.21 | $-1.29 | $10,034.17 | ▲ $10,087.57 (+87.57) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 1 | $13.03 | $0.15 | $-2.07 | $10,047.05 | ▲ $10,087.42 (+87.42) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 3 | $4.79 | $0.17 | $+1.13 | $10,061.25 | ▲ $10,087.25 (+87.25) | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 4 | $3.87 | $0.19 | $-1.61 | $10,076.54 | ▲ $10,087.06 (+87.06) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 1 | $3.92 | $0.06 | $-0.24 | $10,080.40 | ▲ $10,086.90 (+86.90) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 1 | $3.20 | $0.06 | $-0.13 | $10,083.54 | ▲ $10,086.84 (+86.84) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `MRLN` | 1 | $3.30 | $0.06 | $-0.55 | $10,086.79 | ▲ $10,086.79 (+86.79) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,831.06 | ▲ $10,084.61 (+84.61) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,645.90 | ▲ $10,082.58 (+82.58) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 218 | $5.77 | $2.81 | — | $6,385.23 | ▲ $10,079.77 (+79.77) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $5,126.73 | ▲ $10,077.59 (+77.59) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $3,880.15 | ▲ $10,075.47 (+75.47) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 720 | $1.75 | $9.29 | — | $2,610.87 | ▲ $10,066.19 (+66.19) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 170 | $7.38 | $2.50 | — | $1,353.77 | ▲ $10,063.69 (+63.69) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 133 | $9.46 | $2.39 | — | $93.20 | ▲ $10,061.30 (+61.30) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1260.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 3 | $5.43 | $0.17 | — | $76.73 | ▲ $10,662.74 (+662.74) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $18.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 1 | $17.93 | $0.18 | — | $58.62 | ▲ $10,662.56 (+662.56) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $18.64 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 7 | $2.43 | $0.19 | — | $41.42 | ▲ $10,662.37 (+662.37) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $18.64 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 61 | $20.73 | $2.19 | $+6.61 | $1,303.75 | ▲ $10,627.31 (+627.31) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $2,549.05 | ▲ $10,625.26 (+625.26) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 218 | $5.53 | $2.86 | $-57.99 | $3,751.74 | ▲ $10,622.41 (+622.41) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 64 | $21.63 | $2.20 | $+123.61 | $5,133.85 | ▲ $10,620.20 (+620.20) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 42 | $32.76 | $2.14 | $+127.21 | $6,507.63 | ▲ $10,618.06 (+618.06) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 720 | $1.91 | $9.42 | $+96.49 | $7,873.42 | ▲ $10,608.65 (+608.65) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRVI` | 170 | $8.31 | $2.54 | $+153.06 | $9,283.58 | ▲ $10,606.11 (+606.11) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 133 | $9.57 | $2.42 | $+9.82 | $10,553.97 | ▲ $10,603.69 (+603.69) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 159 | $8.28 | $2.47 | — | $9,234.98 | ▲ $10,601.22 (+601.22) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1319.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 814 | $1.62 | $10.50 | — | $7,905.80 | ▲ $10,590.72 (+590.72) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1319.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 206 | $6.38 | $2.66 | — | $6,588.86 | ▲ $10,588.06 (+588.06) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1319.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 38 | $34.48 | $2.10 | — | $5,276.52 | ▲ $10,585.96 (+585.96) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1319.25 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 86 | $15.34 | $2.25 | — | $3,955.03 | ▲ $10,583.71 (+583.71) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1319.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 94 | $13.91 | $2.27 | — | $2,645.22 | ▲ $10,581.44 (+581.44) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.5; leftover $1319.25 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 29 | $44.52 | $2.08 | — | $1,352.06 | ▲ $10,579.36 (+579.36) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+3.5; leftover $1319.25 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 145 | $9.07 | $2.42 | — | $34.48 | ▲ $10,576.93 (+576.93) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; ⚪; ret5=+8.3; leftover $1319.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `EMBC` | 3 | $4.98 | $0.18 | $-1.70 | $49.25 | ▲ $10,849.80 (+849.80) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BEKE` | 1 | $18.14 | $0.20 | $-0.18 | $67.18 | ▲ $10,849.59 (+849.59) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `HITI` | 7 | $2.57 | $0.22 | $+0.57 | $84.95 | ▲ $10,849.37 (+849.37) | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 159 | $8.49 | $2.50 | $+28.42 | $1,432.36 | ▲ $10,844.33 (+844.33) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 814 | $1.74 | $10.65 | $+76.53 | $2,838.07 | ▲ $10,833.68 (+833.68) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 206 | $6.02 | $2.70 | $-79.52 | $4,075.49 | ▲ $10,830.98 (+830.98) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 38 | $33.50 | $2.12 | $-41.47 | $5,346.36 | ▲ $10,828.85 (+828.85) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 86 | $18.50 | $2.27 | $+267.24 | $6,935.09 | ▲ $10,826.58 (+826.58) | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `VIPS` | 94 | $14.00 | $2.30 | $+3.89 | $8,248.79 | ▲ $10,824.28 (+824.28) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RHI` | 29 | $44.41 | $2.10 | $-7.36 | $9,534.58 | ▲ $10,822.18 (+822.18) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUZ` | 145 | $8.88 | $2.46 | $-32.43 | $10,819.72 | ▲ $10,819.72 (+819.72) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $9,473.11 | ▲ $10,817.71 (+817.71) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $127.07 | $2.02 | — | $8,200.39 | ▲ $10,815.69 (+815.69) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $323.82 | $2.00 | — | $6,903.10 | ▲ $10,813.68 (+813.68) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-11.7; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.11 | $2.03 | — | $5,625.53 | ▲ $10,811.65 (+811.65) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-7.4; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.80 | $2.05 | — | $4,304.68 | ▲ $10,809.60 (+809.60) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-7.8; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $303.67 | $2.00 | — | $3,088.00 | ▲ $10,807.60 (+807.60) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-11.1; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $121.13 | $2.02 | — | $1,753.54 | ▲ $10,805.57 (+805.57) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-9.8; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 78 | $17.32 | $2.22 | — | $400.36 | ▲ $10,803.35 (+803.35) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-16.7; leftover $1352.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 9 | $127.63 | $2.04 | $-199.98 | $1,546.99 | ▲ $10,216.25 (+216.25) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 10 | $116.68 | $2.04 | $-107.96 | $2,711.75 | ▲ $10,214.21 (+214.21) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $321.47 | $2.02 | $-13.42 | $3,995.61 | ▲ $10,212.19 (+212.19) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AVT` | 14 | $88.58 | $2.05 | $-39.50 | $5,233.68 | ▲ $10,210.14 (+210.14) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CGNX` | 21 | $59.72 | $2.07 | $-68.81 | $6,485.73 | ▲ $10,208.07 (+208.07) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `COHR` | 4 | $270.50 | $2.02 | $-136.70 | $7,565.70 | ▲ $10,206.04 (+206.04) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LSCC` | 11 | $113.60 | $2.04 | $-86.90 | $8,813.26 | ▲ $10,204.00 (+204.00) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MEI` | 78 | $17.83 | $2.25 | $+35.31 | $10,201.75 | ▲ $10,201.75 (+201.75) | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $8,960.37 | ▲ $10,199.67 (+199.67) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 165 | $7.70 | $2.48 | — | $7,687.39 | ▲ $10,197.19 (+197.19) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $6,425.97 | ▲ $10,195.17 (+195.17) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 55 | $22.78 | $2.15 | — | $5,170.91 | ▲ $10,193.01 (+193.01) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 590 | $2.16 | $7.61 | — | $3,888.90 | ▲ $10,185.40 (+185.40) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 708 | $1.80 | $9.13 | — | $2,605.37 | ▲ $10,176.27 (+176.27) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 109 | $11.63 | $2.32 | — | $1,335.38 | ▲ $10,173.95 (+173.95) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.8; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NEOV` | 348 | $3.66 | $4.49 | — | $57.21 | ▲ $10,169.46 (+169.46) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; 🔵; ⚪; ret5=-8.0; leftover $1275.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 1 | $5.90 | $0.06 | — | $51.25 | ▲ $10,530.36 (+530.36) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=-1.1; leftover $9.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `INO` | 6 | $1.37 | $0.10 | — | $42.93 | ▲ $10,530.26 (+530.26) | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $9.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `DAVE` | cash | leftover split 16.84 < 1 share @ 330.91 |
| 2026-08-14 | `WDC` | cash | leftover split 16.84 < 1 share @ 503.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TGB` | cash | leftover split 5.98 < 1 share @ 8.46 |
| 2026-08-17 | `CDNL` | cash | leftover split 5.98 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 5.98 < 1 share @ 9.12 |
| 2026-08-17 | `OCC` | cash | leftover split 5.98 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 5.98 < 1 share @ 16.20 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `MRLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `MRLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 18.64 < 1 share @ 59.72 |
| 2026-08-21 | `TXG` | cash | leftover split 18.64 < 1 share @ 64.39 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EMBC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BEKE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HITI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `EMBC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BEKE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HITI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `EMBC` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BEKE` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HITI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `VIPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RHI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VIPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RHI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CGNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `COHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LSCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MEI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CGNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `COHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LSCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MEI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NEOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BVS` | cash | leftover split 9.54 < 1 share @ 14.50 |
| 2026-09-04 | `DELL` | cash | leftover split 9.54 < 1 share @ 486.31 |
| 2026-09-04 | `MLYS` | cash | leftover split 9.54 < 1 share @ 29.15 |
| 2026-09-04 | `TARS` | cash | leftover split 9.54 < 1 share @ 82.76 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `HRMY` | 30 | 2026-09-03 @ $41.31 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1275.22 |
| `VSTM` | 165 | 2026-09-03 @ $7.70 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1275.22 |
| `RVTY` | 10 | 2026-09-03 @ $125.94 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1275.22 |
| `MMED` | 55 | 2026-09-03 @ $22.78 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1275.22 |
| `CRDL` | 590 | 2026-09-03 @ $2.16 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1275.22 |
| `BMEA` | 708 | 2026-09-03 @ $1.80 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1275.22 |
| `VIR` | 109 | 2026-09-03 @ $11.63 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.8; leftover $1275.22 |
| `NEOV` | 348 | 2026-09-03 @ $3.66 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; 🔵; ⚪; ret5=-8.0; leftover $1275.22 |
| `LENZ` | 1 | 2026-09-04 @ $5.90 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=-1.1; leftover $9.54 |
| `INO` | 6 | 2026-09-04 @ $1.37 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $9.54 |
