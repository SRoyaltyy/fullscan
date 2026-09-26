# Mechanics check

This file does not re-score a rule. It does not edit `PREREG.md`, `ADDENDUM_1.md`, or `part2_frozen.json`. The ledgers in this folder are the ones already scored.

**No scoring bug found.** Every closed trade that spans a split uses split-adjusted prices. Six other split events in the cache are closer to an unadjusted jump, and none of them is an entry, an exit, or an interior date of a saved trade. Scoring was left as published.

## 1. Split and dividend adjustment

The cache is Yahoo chart v8 `indicators.quote` open, high, low, close, and volume, from the download that requested `events=div|split`. `adj_close` is stored on each bar and is not read by a gate, a rank, a fill, or a fee.

Quote prices are split-adjusted. Volume is split-adjusted in the same units, so `close * volume` stays a dollar amount. Quote prices are not dividend-adjusted. `adj_close` is the dividend-adjusted close. On AAPL the 4-for-1 split is ex-date 2020-08-31. The cached close is 124.81 on 2020-08-28 and the cached open is 127.58 on 2020-08-31. An unadjusted tape would have dropped by about 4x. The close/`adj_close` ratio is about 1.032 on both sides of that split, which is the dividend factor, not the split.

Split events in the cached responses: 2468. Forward (raw price falls): 482. Reverse (raw price rises, numerator < denominator): 1986.

Each split is classified from the cached ex-date open divided by the prior session close. Adjusted means that ratio is closer to 1 than to the raw split multiplier. Unadjusted means it is closer to the raw multiplier. A 0.05 gap in absolute log distance is required; otherwise the split is ambiguous. A missing print is `no_bar`.

| class | all splits | reverse splits |
| --- | --- | --- |
| adjusted | 2326 | 1962 |
| unadjusted | 6 | 6 |
| ambiguous | 123 | 7 |
| no_bar | 13 | 11 |

Reverse splits whose ex-date is a session on which that ticker is in the capped proxy panel: 47. The panel is the Part 1 window (2019-01-02 through 2026-08-12), rebuilt for this count only and not scored.

Closed trades checked: 57496 (Part 1, Part 2 train, and Part 2 test). Fee recomputation mismatches: 0. P&L identity mismatches: 0.

Trades whose open-to-exit interval contains a split ex-date (`entry < ex-date <= exit`): 71. Rebuilding a raw per-share price by undoing the split factor changes the simple return on exactly these 71 trades. Absolute difference: median 2613.76%, max 13280.00%. That gap is the split ratio, not a second price path in the ledger. Of the 71, unadjusted: 0. Ambiguous and not otherwise adjusted: 0. No cached print and not otherwise adjusted: 0.

Example: `XTIA` in the Part 2 test book, entry open 2025-01-08 at 15.00, exit open 2025-01-10 at 8.00. The cached return is 8/15 − 1 = −46.67%. Yahoo records a 1-for-250 reverse split on 2025-01-10, and the cached opens stay on one scale (15.00 then 8.00), so the split is adjusted. The raw per-share reconstruction is 8 / (15/250) − 1 = +13,233.33%. The ledger stores −46.67%. The same pattern, with a different ratio, is the other 70 trades.

The book fills are the cached opens. The recorded return is the split-adjusted return. The raw figure includes the split jump and is not the economic return.

Six events failed the continuity test (observed ex-date open / prior close closer to the raw multiplier than to 1). None is inside a saved trade:

| ticker | ex-date | ratio | raw multiplier | observed open / prior close |
| --- | --- | --- | --- | --- |
| CRVO | 2023-08-17 | 2:3 | 1.50 | 2.05 |
| JL | 2024-12-10 | 1:10 | 10.00 | 10.98 |
| PPCB | 2025-01-29 | 1:2 | 2.00 | 30000.00 |
| VTOL | 2020-06-12 | 1:2 | 2.00 | 1.58 |
| WKSP | 2019-03-29 | 1:6 | 6.00 | 15.87 |
| YAAS | 2026-07-30 | 1:5 | 5.00 | 5.20 |

`JL` and `YAAS` are clean unadjusted reverse splits (the price jumps by about the ratio). `PPCB` is not a missed 1-for-2; the print jumps about 30,000x. `CRVO`, `VTOL`, and `WKSP` are closer to the raw multiplier than to 1, with a leftover gap. They do not change a saved fill.

## 2. Fees and slippage

There is no slippage model. A buy fills at that session's open. A sell fills at the open of the exit session. A missing open carries the lot. It does not substitute a close.

Futubull is `paper_trade.order_fees`, one call per order. A round trip is one buy and one sell. The 15bp column is not a second Futubull charge. It is `0.0015 * shares * entry`, once per closed trade, on the same shares.

For `shares > 0` and `price > 0`, amount = shares * price:

- commission = min(max(0.0049 * shares, 0.99), 0.005 * amount)
- platform = min(max(0.005 * shares, 1.00), 0.005 * amount)
- settlement = 0.003 * shares
- a sell also adds regulatory = max(0.000008 * amount, 0.01)
- a sell also adds TAF = min(max(0.000166 * shares, 0.01), 8.30)

The order fee is that sum, rounded to 4 decimal places. A buy does not pay regulatory or TAF. The ledger stores `buy_fee` and `sell_fee` once each. Closed P&L is `shares * (exit - entry) - buy_fee - sell_fee`. Every checked trade matches a fresh call of `order_fees` and that identity.

Worked trades from the Part 1 ledger:

### longhist_rvol_lg_h1 LCTX 2019-01-30 → 2019-01-31

8442 shares, entry 1.570000, exit 1.400000.

Buy amount 13253.9404. Commission 41.3658 (the per-share rate: 0.0049 * 8442 = 41.3658, above the $0.99 floor and under the 0.5% cap), platform 42.2100, settlement 25.3260. Buy fee 108.9018. Ledger buy fee 108.9018.

Sell amount 11818.7998. Commission 41.3658, platform 42.2100, settlement 25.3260, regulatory 0.0946, TAF 1.4014. Sell fee 110.3977. Ledger sell fee 110.3977.

P&L -1654.4401 = 8442 * (1.400000 - 1.570000) - 108.9018 - 110.3977.

15bp is once on the entry notional: 0.0015 * 13253.9404 = 19.8809. It is not charged again on the sell.

### longhist_break10_h2 TRLV 2019-02-26 → 2019-02-28

1 shares, entry 14.300000, exit 13.350000.

Buy amount 14.3000. Commission 0.0715 (the 0.5% cap: min(max(0.0049, 0.99), 0.0715) = 0.0715, so the $0.99 floor does not bind), platform 0.0715, settlement 0.0030. Buy fee 0.1460. Ledger buy fee 0.1460.

Sell amount 13.3500. Commission 0.0668, platform 0.0668, settlement 0.0030, regulatory 0.0100, TAF 0.0100. Sell fee 0.1565. Ledger sell fee 0.1565.

P&L -1.2525 = 1 * (13.350000 - 14.300000) - 0.1460 - 0.1565.

15bp is once on the entry notional: 0.0015 * 14.3000 = 0.0215. It is not charged again on the sell.

### longhist_rvol_lg_h1 GEVO 2020-11-20 → 2020-11-23

8 shares, entry 1.390000, exit 1.430000.

Buy amount 11.1200. Commission 0.0556 (again the 0.5% cap, not the $0.99 floor), platform 0.0556, settlement 0.0240. Buy fee 0.1352. Ledger buy fee 0.1352.

Sell amount 11.4400. Commission 0.0572, platform 0.0572, settlement 0.0240, regulatory 0.0100, TAF 0.0100. Sell fee 0.1584. Ledger sell fee 0.1584.

P&L 0.0264 = 8 * (1.430000 - 1.390000) - 0.1352 - 0.1584.

15bp is once on the entry notional: 0.0015 * 11.1200 = 0.0167. It is not charged again on the sell.

## 3. Sizing

Position size is leftover cash, not a fraction of equity and not a fixed dollar ticket. `day_cap` is 1. After the morning sells, the cash balance is split equally across the new names (names already held are excluded). Shares = floor(budget / open). A name under 1 share is skipped and its budget is not given to another name. If the buy fee makes the cost exceed cash, shares are reduced with `order_fees` until the ticket fits or the name is skipped. This is the `leftover` rule in `PREREG.md` section 5. `simulate` in `research/longhist/engine.py` does that and does not size off marked equity.

The published books start at $10,000 and reinvest what is left. The −99% ending equity is that compounded path. It is not a fixed-ticket average.

The dollar minimums do not grow as a share of a tiny ticket. Commission is `min(max(0.0049 * shares, 0.99), 0.005 * amount)`. When the ticket is small, `0.005 * amount` is below $0.99, so the 0.5% cap binds and the $0.99 floor does not. The same is true of the $1.00 platform floor. The floor binds only when the notional is still large enough that 0.5% of amount exceeds the floor (about $198 for commission) and the per-share rate is still below the floor (under about 202 shares). The `TRLV` and `GEVO` examples above are cap-bound, not floor-bound.

The table uses the saved ledgers. `price P&L` is `shares * (exit - entry)` on those shares. `fees` is buy fee plus sell fee. Fee rate is that sum divided by entry notional. `floor premium` is the part of the fee above the same formula with the per-order minimums removed and the percentage caps kept. `fixed $10k` resizes each saved trade to a fresh $10,000 ticket (whole shares, Futubull once per side). It is a diagnostic. It is not a pass-rule result and it does not replace the published return. The pooled rows add separate $10,000 books.

| book | trades | price P&L | fees | floor premium | fee rate if shares < 200 | fee rate if shares ≥ 200 | trades with shares < 200 | fixed $10k mean Futubull return | fixed $10k floor premium |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Part 1, four rules pooled | 3243 | -25,508.85 | 14,363.18 | 2,859.66 | 0.52% | 0.50% | 2726 | -0.50% | 4.65 |
| Part 2 test, 40 rules pooled | 28761 | -156,552.18 | 227,243.09 | 22,964.11 | 0.62% | 0.55% | 22081 | -0.27% | 9.73 |
| longhist_break10_h2 | 565 | -8,476.27 | 1,519.88 | 321.05 | 0.66% | 0.48% | 521 | -0.87% | 0.00 |
| longhist_rvol_lg_h1 | 1716 | -1,469.90 | 8,525.92 | 1,773.73 | 0.49% | 0.52% | 1397 | -0.27% | 4.65 |
| longhist_break10_h1 | 833 | -7,190.11 | 2,808.57 | 626.70 | 0.56% | 0.50% | 739 | -0.53% | 0.00 |
| longhist_zero_candle_h2 | 129 | -8,372.57 | 1,508.81 | 138.18 | 0.58% | 0.43% | 69 | -1.86% | 0.00 |

Before fees, per-trade return does not depend on the share count. The fixed $10k ticket changes the Futubull return, because the minimums shrink as a fraction of a $10,000 order. It does not change the 15bp return, which is a flat fraction of entry notional. The dollar gap between price P&L and published closed P&L is the fee total above. The floor premium is only the slice of that fee that comes from the dollar minimums. On a tiny ticket the 0.5% cap replaces those minimums, which is why the fee rate below 200 shares stays near the fee rate above 200 shares.

Part 1, four rules pooled: closed loss 39,872.02. Price movement is 25,508.85 of that loss (64.0%). Fees are 14,363.18 (36.0%). The minimum-floor premium is 2,859.66 (7.2% of the closed loss).

Part 2 test, 40 rules pooled: closed loss 383,795.27. Price movement is 156,552.18 of that loss (40.8%). Fees are 227,243.09 (59.2%). The minimum-floor premium is 22,964.11 (6.0% of the closed loss).

longhist_break10_h2: closed loss 9,996.15. Price movement is 8,476.27 of that loss (84.8%). Fees are 1,519.88 (15.2%). The minimum-floor premium is 321.05 (3.2% of the closed loss).

longhist_rvol_lg_h1: closed loss 9,995.82. Price movement is 1,469.90 of that loss (14.7%). Fees are 8,525.92 (85.3%). The minimum-floor premium is 1,773.73 (17.7% of the closed loss).

longhist_break10_h1: closed loss 9,998.68. Price movement is 7,190.11 of that loss (71.9%). Fees are 2,808.57 (28.1%). The minimum-floor premium is 626.70 (6.3% of the closed loss).

longhist_zero_candle_h2: closed loss 9,881.38. Price movement is 8,372.57 of that loss (84.7%). Fees are 1,508.81 (15.3%). The minimum-floor premium is 138.18 (1.4% of the closed loss).

The minimum-floor premium is 1.4% to 17.7% of each Part 1 book's dollar loss. It is not the source of the −99% ending equity. Resizing the same trades to a fixed $10,000 ticket, the mean Futubull return per trade is −0.87%, −0.27%, −0.53%, and −1.86% for the four Part 1 rules. Compounding a per-trade result of that size, fully reinvested, over hundreds of trades is what takes a $10,000 book to a few dollars. On `longhist_rvol_lg_h1` the ordinary per-share schedule, not the dollar floor, is 85.3% of the dollar loss, because that rule closes 1,716 trades. The fee rate stays about 0.5% of notional below 200 shares and about 0.5% above it.

## 4. Per-trade mean return, 95% CI

Each closed trade is one observation. The return before fees is `(exit - entry) / entry`. The return after 15bp is that minus 0.0015. The interval is the Student-t 95% interval for the mean, `mean ± t_{n-1, 0.975} * s / sqrt(n)`. It is not the entry-day cluster test in rule 6.1, and it is not a Holm test. Open lots at the window end are not trades.

Because 15bp subtracts a constant, the after-fee interval is the before-fee interval shifted down by 0.15 percentage points.

| rule | n | mean before fees | 95% CI | mean after 15bp | 95% CI |
| --- | --- | --- | --- | --- | --- |
| longhist_break10_h1 | 833 | 0.17% | -1.01% to 1.35% | 0.02% | -1.16% to 1.20% |
| longhist_break10_h2 | 565 | 0.14% | -1.50% to 1.77% | -0.01% | -1.65% to 1.62% |
| longhist_rvol_lg_h1 | 1716 | 0.44% | -0.35% to 1.24% | 0.29% | -0.50% to 1.09% |
| longhist_zero_candle_h2 | 129 | -1.35% | -4.47% to 1.77% | -1.50% | -4.62% to 1.62% |
| RANDOM4 part 1 | 1828215 | 0.72% | 0.70% to 0.74% | 0.57% | 0.55% to 0.59% |

Part 2 test rules, same definition. RANDOM4 here is the test-window baseline, seed 20260813, 1,000 draws, hold 1, four names.

| rule | n | mean before fees | 95% CI | mean after 15bp | 95% CI |
| --- | --- | --- | --- | --- | --- |
| longhist2_break10_h1_n2_candle_score | 613 | 0.80% | -2.61% to 4.21% | 0.65% | -2.76% to 4.06% |
| longhist2_break10_h1_n2_hot_score | 643 | 0.93% | -2.46% to 4.31% | 0.78% | -2.61% to 4.16% |
| longhist2_break10_h1_n4_candle_score | 1133 | 0.48% | -1.50% to 2.47% | 0.33% | -1.65% to 2.32% |
| longhist2_break10_h1_n4_hot_score | 1329 | 0.55% | -1.22% to 2.32% | 0.40% | -1.37% to 2.17% |
| longhist2_break10_h2_n2_candle_score | 713 | 0.21% | -2.73% to 3.15% | 0.06% | -2.88% to 3.00% |
| longhist2_break10_h2_n2_hot_score | 437 | -0.29% | -4.76% to 4.18% | -0.44% | -4.91% to 4.03% |
| longhist2_break10_h2_n4_candle_score | 1115 | 0.20% | -1.93% to 2.32% | 0.05% | -2.08% to 2.17% |
| longhist2_break10_h2_n4_hot_score | 1126 | 0.29% | -1.87% to 2.46% | 0.14% | -2.02% to 2.31% |
| longhist2_pullback_h1_n2_candle_score | 497 | -0.76% | -2.17% to 0.66% | -0.91% | -2.32% to 0.51% |
| longhist2_pullback_h1_n2_hot_score | 520 | -0.73% | -2.10% to 0.64% | -0.88% | -2.25% to 0.49% |
| longhist2_pullback_h1_n4_candle_score | 620 | -0.67% | -1.85% to 0.52% | -0.82% | -2.00% to 0.37% |
| longhist2_pullback_h1_n4_hot_score | 611 | -0.70% | -1.88% to 0.48% | -0.85% | -2.03% to 0.33% |
| longhist2_pullback_h2_n2_candle_score | 476 | 3.34% | -3.06% to 9.73% | 3.19% | -3.21% to 9.58% |
| longhist2_pullback_h2_n2_hot_score | 481 | 3.69% | -2.66% to 10.03% | 3.54% | -2.81% to 9.88% |
| longhist2_pullback_h2_n4_candle_score | 597 | 2.93% | -2.23% to 8.09% | 2.78% | -2.38% to 7.94% |
| longhist2_pullback_h2_n4_hot_score | 595 | 2.81% | -2.34% to 7.97% | 2.66% | -2.49% to 7.82% |
| longhist2_ret5_up_h1_n2_candle_score | 653 | 0.64% | -2.59% to 3.86% | 0.49% | -2.74% to 3.71% |
| longhist2_ret5_up_h1_n2_hot_score | 749 | 1.17% | -1.82% to 4.16% | 1.02% | -1.97% to 4.01% |
| longhist2_ret5_up_h1_n4_candle_score | 1399 | 0.30% | -1.38% to 1.97% | 0.15% | -1.53% to 1.82% |
| longhist2_ret5_up_h1_n4_hot_score | 1758 | 0.67% | -0.79% to 2.13% | 0.52% | -0.94% to 1.98% |
| longhist2_ret5_up_h2_n2_candle_score | 562 | 0.55% | -3.01% to 4.11% | 0.40% | -3.16% to 3.96% |
| longhist2_ret5_up_h2_n2_hot_score | 581 | 0.69% | -3.00% to 4.38% | 0.54% | -3.15% to 4.23% |
| longhist2_ret5_up_h2_n4_candle_score | 1215 | -0.76% | -2.00% to 0.48% | -0.91% | -2.15% to 0.33% |
| longhist2_ret5_up_h2_n4_hot_score | 1312 | -0.18% | -1.60% to 1.24% | -0.33% | -1.75% to 1.09% |
| longhist2_rvol_lg_h1_n2_candle_score | 633 | 0.75% | -2.54% to 4.04% | 0.60% | -2.69% to 3.89% |
| longhist2_rvol_lg_h1_n2_hot_score | 701 | 1.07% | -2.05% to 4.19% | 0.92% | -2.20% to 4.04% |
| longhist2_rvol_lg_h1_n4_candle_score | 1309 | 0.06% | -1.67% to 1.80% | -0.09% | -1.82% to 1.65% |
| longhist2_rvol_lg_h1_n4_hot_score | 1479 | 0.46% | -1.17% to 2.10% | 0.31% | -1.32% to 1.95% |
| longhist2_rvol_lg_h2_n2_candle_score | 686 | 0.12% | -2.87% to 3.12% | -0.03% | -3.02% to 2.97% |
| longhist2_rvol_lg_h2_n2_hot_score | 680 | 0.87% | -2.33% to 4.07% | 0.72% | -2.48% to 3.92% |
| longhist2_rvol_lg_h2_n4_candle_score | 1240 | -0.38% | -2.19% to 1.42% | -0.53% | -2.34% to 1.27% |
| longhist2_rvol_lg_h2_n4_hot_score | 1770 | 1.11% | -0.56% to 2.78% | 0.96% | -0.71% to 2.63% |
| longhist2_zero_px_h1_n2_candle_score | 69 | -0.30% | -3.28% to 2.68% | -0.45% | -3.43% to 2.53% |
| longhist2_zero_px_h1_n2_hot_score | 69 | -0.64% | -3.69% to 2.42% | -0.79% | -3.84% to 2.27% |
| longhist2_zero_px_h1_n4_candle_score | 70 | -0.63% | -3.64% to 2.38% | -0.78% | -3.79% to 2.23% |
| longhist2_zero_px_h1_n4_hot_score | 70 | -0.63% | -3.64% to 2.38% | -0.78% | -3.79% to 2.23% |
| longhist2_zero_px_h2_n2_candle_score | 62 | -1.05% | -5.63% to 3.54% | -1.20% | -5.78% to 3.39% |
| longhist2_zero_px_h2_n2_hot_score | 62 | -1.74% | -6.43% to 2.94% | -1.89% | -6.58% to 2.79% |
| longhist2_zero_px_h2_n4_candle_score | 63 | -1.41% | -5.98% to 3.15% | -1.56% | -6.13% to 3.00% |
| longhist2_zero_px_h2_n4_hot_score | 63 | -1.78% | -6.39% to 2.82% | -1.93% | -6.54% to 2.67% |
| RANDOM4 part 2 test | 1758067 | 0.23% | 0.20% to 0.27% | 0.08% | 0.05% to 0.12% |

Part 1 RANDOM4 regenerated ending-equity mean return -99.96%. Published mean return -99.96%.

Part 2 test RANDOM4 regenerated ending-equity mean return -99.93%. Published mean return -99.93%.

