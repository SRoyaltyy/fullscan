# Portfolio backtest — 2026-08-31

Actual buy/sell simulation over the historical signal ledger with position sizing, capital caps and Futubull fees ($0.0049/sh min $0.99 commission + $0.005/sh min $1 platform + SEC/TAF sell-side + 5bps slippage + 1%/yr short borrow). Open positions are marked at cost (see caveats).

| scenario | final equity | total ret | CAGR | max DD* | Sharpe | win% | trades | skipped | fees paid |
|---|---|---|---|---|---|---|---|---|---|
| A_all_signals | $649,520 | +29.9% | +7.0% | -8.7% | 0.87 | 51.5% | 14707 | 239581 | $600,610 |
| B_top3_strength | $351,274 | +17.1% | +4.1% | -14.1% | 0.47 | 48.0% | 2928 | 5 | $111,373 |
| C_long_only_top3 | $281,232 | +40.6% | +9.2% | -22.0% | 0.69 | 47.9% | 2927 | 49 | $105,793 |
| D_short_only_top5 | $206,187 | -17.5% | -4.9% | -36.4% | -0.36 | 49.0% | 4839 | 65 | $290,158 |
| E_sleeves | $598,497 | +14.0% | +3.4% | -23.5% | 0.35 | 50.2% | 17469 | 236819 | $754,993 |
| F_best_pair_L2_S1 | $156,830 | -37.3% | -11.3% | -39.7% | -1.75 | 53.0% | 2915 | 1736 | $121,693 |

\* drawdown on cost-basis equity — understated for long-hold tp strategies.

## A_all_signals — Every signal from all 7 strategies, $10k each, max 40 open

fees were 22.3% of gross winning P&L

best: BBAI 2023-01-23 LONG $8,590; AMPX 2024-12-24 LONG $8,569; CAR 2026-03-23 LONG $7,721

worst: ATCH 2025-09-23 SHORT $-7,010; AGPU 2025-09-26 SHORT $-5,500; ASBP 2026-03-24 SHORT $-4,205

## B_top3_strength — Top 3 signals/day ranked by highlight strength, $10k each

fees were 12.6% of gross winning P&L

best: DJT 2024-01-18 LONG $9,389; RGTI 2025-09-11 LONG $8,831; CLOV 2024-08-12 LONG $7,904

worst: MAAS 2025-04-29 LONG $-5,256; IFBD 2022-12-21 SHORT $-3,171; NAK 2024-01-05 LONG $-2,900

## C_long_only_top3 — Longs only (L1-L5), top 3/day by strength, $10k each

fees were 11.4% of gross winning P&L

best: SYRE 2023-06-20 LONG $21,399; DJT 2024-01-18 LONG $9,389; RGTI 2025-09-11 LONG $8,831

worst: MAAS 2025-04-29 LONG $-5,256; NAK 2024-01-05 LONG $-2,900; LSPD 2024-02-06 LONG $-2,873

## D_short_only_top5 — Shorts only (S1-S2), top 5/day by redness, $10k each

fees were 31.2% of gross winning P&L

best: HUBC 2026-04-20 SHORT $5,431; JTAI 2026-04-02 SHORT $4,745; DUO 2023-07-17 SHORT $4,682

worst: CD 2025-07-23 SHORT $-5,370; CD 2025-07-23 SHORT $-5,370; VIVK 2026-07-20 SHORT $-4,979

## E_sleeves — $75k sleeve per strategy, $10k/trade, max 7 open per sleeve

fees were 22.5% of gross winning P&L

best: HTZ 2025-04-15 LONG $13,272; EOSE 2024-06-21 LONG $10,549; CLSK 2023-11-21 LONG $9,546

worst: CMCT 2025-01-03 SHORT $-86,879; QXL 2023-04-14 SHORT $-22,642; DUKR 2022-12-22 SHORT $-8,432

## F_best_pair_L2_S1 — Only L2 (tp3 longs) + S1 (1-day shorts), top 3/day each side

fees were 26.8% of gross winning P&L

best: STEX 2024-06-11 SHORT $4,251; SKYA 2025-03-28 SHORT $2,842; NRT 2023-10-31 SHORT $2,695

worst: OGN 2025-04-29 LONG $-3,070; DFDV 2025-06-27 SHORT $-2,758; FATE 2023-01-06 SHORT $-2,460

