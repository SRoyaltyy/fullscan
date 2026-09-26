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

## 5. Worst days and position count

This section reads the saved Part 1 ledgers. It does not rescore them. The proxy panel cap of 60 is the candidate list in PREREG section 3. Part 1 `top_n` is 4, and `top_n` is the length of the look list. Hold 1 sells the prior cohort at the open before the new buys, so the end-of-day book is at most 4 names. Hold 2 can still be holding the prior cohort, so the end-of-day book is at most 8 names. A recorded count of 1–3 is that sizing when the gate qualifies fewer than 4 names, or when leftover cash buys fewer than 1 share of a later name.

### longhist_break10_h2

Maximum end-of-day positions 6. First close under $1,000 is 2019-06-25 at $819.46. Position counts over the full window: 0: 1186, 1: 483, 2: 139, 3: 65, 4: 29, 5: 8, 6: 3. Through 2019-08-30: 0: 33, 1: 67, 2: 37, 3: 15, 4: 11, 5: 4, 6: 1.

| date | daily return | equity | positions | cash | ticker | role | shares | entry | entry px | exit | exit px | day open | day close | prior close | split on the hold | bad bar |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2020-03-13 | -36.46% | 45.52 | 1 | 0.08 | TLSA | held at close | 34 | 2020-03-12 | 2.300920 | 2020-03-16 | 1.000400 | 1.760704 | 1.336535 | 2.104842 | no | no |
| 2019-03-08 | -33.52% | 7,222.49 | 2 | 8.09 | DBVT | sold at open | 638 | 2019-03-06 | 17.500000 | 2019-03-08 | 17.000000 | 17.000000 | 16.719999 | 17.000000 | no | no |
|  |  |  |  |  | ALT | held at close | 1495 | 2019-03-08 | 3.630000 | 2019-03-12 | 2.780000 | 3.630000 | 2.880000 | 4.540000 | no | no |
|  |  |  |  |  | CRDF | held at close | 606 | 2019-03-08 | 8.900000 | 2019-03-12 | 4.420000 | 8.900000 | 4.800000 | 4.130000 | no | no |
| 2020-03-26 | -29.10% | 29.24 | 1 | 0.84 | CAPR | held at close | 20 | 2020-03-25 | 1.620000 | 2020-03-27 | 1.390000 | 1.440000 | 1.420000 | 2.020000 | no | no |
| 2024-10-11 | -28.52% | 6.62 | 2 | 2.41 | SRFM | held at close | 1 | 2024-10-11 | 2.750000 | 2024-10-15 | 1.990000 | 2.750000 | 2.050000 | 3.270000 | no | no |
|  |  |  |  |  | TURB | held at close | 1 | 2024-10-11 | 4.030000 | 2024-10-15 | 1.720000 | 4.030000 | 2.160000 | 3.030000 | no | no |
| 2020-03-16 | -26.12% | 33.63 | 0 | 33.63 | TLSA | sold at open | 34 | 2020-03-12 | 2.300920 | 2020-03-16 | 1.000400 | 1.000400 | 0.900360 | 1.336535 | no | no |
| 2019-06-25 | -23.89% | 819.46 | 2 | 6.00 | DMAC | sold at open | 234 | 2019-06-21 | 4.520000 | 2019-06-25 | 4.560000 | 4.560000 | 4.860000 | 4.600000 | no | no |
|  |  |  |  |  | IGC | held at close | 227 | 2019-06-25 | 2.340000 | 2019-06-27 | 1.740000 | 2.340000 | 1.680000 | 2.050000 | no | no |
|  |  |  |  |  | VIVO | held at close | 29 | 2019-06-25 | 18.000000 | 2019-06-27 | 14.000000 | 18.000000 | 14.900000 | 20.500000 | no | no |
| 2024-01-04 | -21.84% | 3.30 | 1 | 0.48 | ABVC | held at close | 2 | 2024-01-04 | 1.850000 | 2024-01-08 | 1.370000 | 1.850000 | 1.410000 | 1.930000 | no | no |
| 2022-12-20 | -20.51% | 3.57 | 1 | 1.88 | BYSI | held at close | 1 | 2022-12-19 | 2.120000 | 2022-12-21 | 1.940000 | 2.490000 | 1.690000 | 2.610000 | no | no |
| 2020-03-03 | -20.51% | 108.16 | 2 | 0.88 | LRN | sold at open | 1 | 2020-02-28 | 19.770000 | 2020-03-03 | 19.480000 | 19.480000 | 20.330000 | 19.309999 | no | no |
|  |  |  |  |  | SPPP | sold at open | 1 | 2020-02-28 | 18.080000 | 2020-03-03 | 16.809999 | 16.809999 | 16.940001 | 16.750000 | no | no |
|  |  |  |  |  | IFRX | held at close | 8 | 2020-03-03 | 5.210000 | 2020-03-05 | 4.510000 | 5.210000 | 4.590000 | 6.080000 | no | no |
|  |  |  |  |  | TOMZ | held at close | 3 | 2020-03-02 | 29.040001 | 2020-03-04 | 23.760000 | 29.520000 | 23.520000 | 30.959999 | no | no |
| 2025-03-11 | -18.83% | 4.59 | 1 | 1.19 | ALXO | sold at open | 2 | 2025-03-07 | 1.250000 | 2025-03-11 | 0.910000 | 0.910000 | 0.973000 | 0.894000 | no | no |
|  |  |  |  |  | TLSA | sold at open | 2 | 2025-03-07 | 1.580000 | 2025-03-11 | 1.530000 | 1.530000 | 1.370000 | 1.550000 | no | no |
|  |  |  |  |  | SNOA | held at close | 1 | 2025-03-11 | 4.310000 | 2025-03-13 | 3.530000 | 4.310000 | 3.400000 | 2.790000 | no | no |

Dollar contributions of those rows (sold names at the open gap minus the sell fee, new buys at close minus entry minus the buy fee, carried names at the close-to-prior-close move) match the published equity change within $0.0000 on these ten days.

### longhist_rvol_lg_h1

Maximum end-of-day positions 4. First close under $1,000 is 2020-05-29 at $998.52. Position counts over the full window: 0: 1121, 1: 362, 2: 137, 3: 92, 4: 201. Through 2019-08-30: 0: 30, 1: 46, 2: 37, 3: 25, 4: 30.

| date | daily return | equity | positions | cash | ticker | role | shares | entry | entry px | exit | exit px | day open | day close | prior close | split on the hold | bad bar |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2020-09-25 | -32.32% | 84.88 | 3 | 8.62 | POLA | sold at open | 4 | 2020-09-24 | 29.120001 | 2020-09-25 | 19.600000 | 19.600000 | 21.700001 | 25.830000 | no | no |
|  |  |  |  |  | CBAT | held at close | 9 | 2020-09-25 | 3.500000 | 2020-09-28 | 2.790000 | 3.500000 | 2.450000 | 1.350000 | no | no |
|  |  |  |  |  | FLR | held at close | 3 | 2020-09-25 | 8.860000 | 2020-09-28 | 9.640000 | 8.860000 | 9.590000 | 9.350000 | no | no |
|  |  |  |  |  | PPSI | held at close | 16 | 2020-09-25 | 2.000000 | 2020-09-28 | 1.590000 | 2.000000 | 1.590000 | 1.830000 | no | no |
| 2019-03-08 | -27.50% | 8,160.73 | 3 | 1.84 | PETZ | sold at open | 574 | 2019-03-07 | 21.400000 | 2019-03-08 | 18.799999 | 18.799999 | 17.980000 | 19.600000 | no | no |
|  |  |  |  |  | ALT | held at close | 990 | 2019-03-08 | 3.630000 | 2019-03-11 | 2.850000 | 3.630000 | 2.880000 | 4.540000 | no | no |
|  |  |  |  |  | CRDF | held at close | 404 | 2019-03-08 | 8.900000 | 2019-03-11 | 5.320000 | 8.900000 | 4.800000 | 4.130000 | no | no |
|  |  |  |  |  | SWBI | held at close | 440 | 2019-03-08 | 8.124520 | 2019-03-11 | 7.724827 | 8.124520 | 7.655650 | 8.739431 | no | no |
| 2021-04-19 | -25.29% | 18.57 | 1 | 2.07 | WIT | sold at open | 7 | 2021-04-16 | 3.480000 | 2021-04-19 | 3.490000 | 3.490000 | 3.480000 | 3.480000 | no | no |
|  |  |  |  |  | LFMD | held at close | 2 | 2021-04-19 | 11.170000 | 2021-04-20 | 8.000000 | 11.170000 | 8.250000 | 9.970000 | no | no |
| 2020-06-09 | -24.08% | 867.18 | 4 | 3.69 | AMPY | sold at open | 105 | 2020-06-08 | 2.400000 | 2020-06-09 | 2.120000 | 2.120000 | 2.000000 | 2.230000 | no | no |
|  |  |  |  |  | CHRD | sold at open | 159 | 2020-06-08 | 1.620000 | 2020-06-09 | 1.650000 | 1.650000 | 1.570000 | 2.040000 | no | no |
|  |  |  |  |  | SMHI | sold at open | 70 | 2020-06-08 | 3.640000 | 2020-06-09 | 4.500000 | 4.500000 | 4.390000 | 4.790000 | no | no |
|  |  |  |  |  | TDAY | sold at open | 85 | 2020-06-08 | 3.030000 | 2020-06-09 | 2.800000 | 2.800000 | 2.700000 | 2.910000 | no | no |
|  |  |  |  |  | BORR | held at close | 54 | 2020-06-09 | 4.740000 | 2020-06-10 | 3.200000 | 4.740000 | 3.560000 | 5.260000 | no | no |
|  |  |  |  |  | MITT | held at close | 12 | 2020-06-09 | 21.209999 | 2020-06-10 | 19.049999 | 21.209999 | 17.340000 | 22.709999 | no | no |
|  |  |  |  |  | SMHI | held at close | 57 | 2020-06-09 | 4.500000 | 2020-06-10 | 4.400000 | 4.500000 | 4.390000 | 4.790000 | no | no |
|  |  |  |  |  | SRG | held at close | 13 | 2020-06-09 | 19.280001 | 2020-06-10 | 16.650000 | 19.280001 | 16.379999 | 21.240000 | no | no |
| 2020-06-24 | -21.30% | 541.65 | 3 | 12.63 | HYLN | sold at open | 9 | 2020-06-23 | 19.440001 | 2020-06-24 | 17.410000 | 17.410000 | 16.209999 | 17.400000 | no | no |
|  |  |  |  |  | MVIS | sold at open | 7 | 2020-06-23 | 24.600000 | 2020-06-24 | 20.700001 | 20.700001 | 21.900000 | 22.049999 | no | no |
|  |  |  |  |  | NEON | sold at open | 19 | 2020-06-23 | 9.270000 | 2020-06-24 | 8.000000 | 8.000000 | 8.200000 | 8.190000 | no | no |
|  |  |  |  |  | VNET | sold at open | 8 | 2020-06-23 | 22.420000 | 2020-06-24 | 23.190001 | 23.190001 | 22.209999 | 23.750000 | no | no |
|  |  |  |  |  | KMDA | held at close | 22 | 2020-06-24 | 9.910000 | 2020-06-25 | 9.250000 | 9.910000 | 9.450000 | 8.940000 | no | no |
|  |  |  |  |  | LE | held at close | 24 | 2020-06-24 | 8.910000 | 2020-06-25 | 7.770000 | 8.910000 | 7.880000 | 9.400000 | no | no |
|  |  |  |  |  | YHGJ | held at close | 4 | 2020-06-24 | 53.400002 | 2020-06-25 | 28.900000 | 53.400002 | 33.000000 | 23.600000 | no | no |
| 2020-08-14 | -17.79% | 279.83 | 4 | 8.03 | AMTX | sold at open | 31 | 2020-08-13 | 2.800000 | 2020-08-14 | 2.400000 | 2.400000 | 2.450000 | 2.790000 | no | no |
|  |  |  |  |  | CCO | sold at open | 66 | 2020-08-13 | 1.340000 | 2020-08-14 | 1.180000 | 1.180000 | 1.200000 | 1.130000 | no | no |
|  |  |  |  |  | OPTT | sold at open | 60 | 2020-08-13 | 1.470000 | 2020-08-14 | 1.380000 | 1.380000 | 1.180000 | 1.480000 | no | no |
|  |  |  |  |  | TIGR | sold at open | 13 | 2020-08-13 | 6.540000 | 2020-08-14 | 6.620000 | 6.620000 | 6.520000 | 6.760000 | no | no |
|  |  |  |  |  | AHG | held at close | 18 | 2020-08-14 | 4.260000 | 2020-08-17 | 2.940000 | 4.260000 | 3.180000 | 7.440000 | no | no |
|  |  |  |  |  | FOSL | held at close | 15 | 2020-08-14 | 5.080000 | 2020-08-17 | 5.490000 | 5.080000 | 5.620000 | 5.300000 | no | no |
|  |  |  |  |  | OPTT | held at close | 57 | 2020-08-14 | 1.380000 | 2020-08-17 | 1.500000 | 1.380000 | 1.180000 | 1.480000 | no | no |
|  |  |  |  |  | SNDL | held at close | 14 | 2020-08-14 | 5.500000 | 2020-08-17 | 4.840000 | 5.500000 | 4.500000 | 7.000000 | no | no |
| 2020-07-01 | -16.24% | 403.02 | 4 | 2.19 | BLNK | sold at open | 21 | 2020-06-30 | 5.910000 | 2020-07-01 | 4.700000 | 4.700000 | 4.840000 | 5.680000 | no | no |
|  |  |  |  |  | CLSK | sold at open | 45 | 2020-06-30 | 2.830000 | 2020-07-01 | 2.450000 | 2.450000 | 2.360000 | 2.590000 | no | no |
|  |  |  |  |  | ERII | sold at open | 16 | 2020-06-30 | 7.700000 | 2020-07-01 | 7.610000 | 7.610000 | 7.010000 | 7.600000 | no | no |
|  |  |  |  |  | PDSB | sold at open | 59 | 2020-06-30 | 2.140000 | 2020-07-01 | 1.850000 | 1.850000 | 1.720000 | 2.010000 | no | no |
|  |  |  |  |  | GNW | held at close | 48 | 2020-07-01 | 2.210000 | 2020-07-02 | 2.310000 | 2.210000 | 2.220000 | 2.310000 | no | no |
|  |  |  |  |  | HIMX | held at close | 25 | 2020-07-01 | 4.400000 | 2020-07-02 | 4.170000 | 4.400000 | 4.020000 | 4.140000 | no | no |
|  |  |  |  |  | OPK | held at close | 29 | 2020-07-01 | 3.720000 | 2020-07-02 | 3.500000 | 3.720000 | 3.430000 | 3.410000 | no | no |
|  |  |  |  |  | OXBR | held at close | 82 | 2020-07-01 | 1.330000 | 2020-07-02 | 1.110000 | 1.330000 | 1.150000 | 1.400000 | no | no |
| 2022-01-06 | -16.17% | 8.71 | 1 | 6.73 | AACG | sold at open | 4 | 2022-01-05 | 1.800000 | 2022-01-06 | 1.890000 | 1.890000 | 1.980000 | 2.300000 | no | no |
|  |  |  |  |  | AACG | held at close | 1 | 2022-01-06 | 1.890000 | 2022-01-07 | 1.930000 | 1.890000 | 1.980000 | 2.300000 | no | no |
| 2020-06-17 | -15.93% | 628.00 | 4 | 56.78 | ALAR | sold at open | 11 | 2020-06-16 | 18.000000 | 2020-06-17 | 17.600000 | 17.600000 | 17.200001 | 17.000000 | no | no |
|  |  |  |  |  | BYRN | sold at open | 12 | 2020-06-16 | 17.200001 | 2020-06-17 | 14.700000 | 14.700000 | 14.600000 | 14.800000 | no | no |
|  |  |  |  |  | COCP | sold at open | 11 | 2020-06-16 | 18.360001 | 2020-06-17 | 16.200001 | 16.200001 | 15.600000 | 16.320000 | no | no |
|  |  |  |  |  | SWBI | sold at open | 13 | 2020-06-16 | 15.126826 | 2020-06-17 | 14.373559 | 14.373559 | 13.927748 | 14.096849 | no | no |
|  |  |  |  |  | CLIR | held at close | 6 | 2020-06-17 | 29.900000 | 2020-06-18 | 23.600000 | 29.900000 | 25.500000 | 23.700001 | no | no |
|  |  |  |  |  | JFIN | held at close | 34 | 2020-06-17 | 5.490000 | 2020-06-18 | 5.000000 | 5.490000 | 5.300000 | 5.770000 | no | no |
|  |  |  |  |  | UONEK | held at close | 2 | 2020-06-17 | 68.000000 | 2020-06-18 | 23.000000 | 68.000000 | 26.500000 | 27.400000 | no | no |
|  |  |  |  |  | WAFU | held at close | 29 | 2020-06-17 | 6.250000 | 2020-06-18 | 5.920000 | 6.250000 | 6.380000 | 7.600000 | no | no |
| 2019-04-11 | -15.74% | 5,601.63 | 3 | 17.01 | AACG | sold at open | 1035 | 2019-04-10 | 2.570000 | 2019-04-11 | 3.000000 | 3.000000 | 3.050000 | 3.850000 | no | no |
|  |  |  |  |  | SGI | sold at open | 167 | 2019-04-10 | 15.812500 | 2019-04-11 | 15.890000 | 15.890000 | 16.070000 | 15.915000 | no | no |
|  |  |  |  |  | AACG | held at close | 638 | 2019-04-11 | 3.000000 | 2019-04-12 | 2.860000 | 3.000000 | 3.050000 | 3.850000 | no | no |
|  |  |  |  |  | MARA | held at close | 563 | 2019-04-11 | 3.400000 | 2019-04-12 | 3.250000 | 3.400000 | 3.190000 | 3.860000 | no | no |
|  |  |  |  |  | RCEL | held at close | 65 | 2019-04-11 | 29.000000 | 2019-04-12 | 25.799999 | 29.000000 | 28.350000 | 27.100000 | no | no |

Dollar contributions of those rows (sold names at the open gap minus the sell fee, new buys at close minus entry minus the buy fee, carried names at the close-to-prior-close move) match the published equity change within $0.0000 on these ten days.

### longhist_break10_h1

Maximum end-of-day positions 4. First close under $1,000 is 2019-08-29 at $787.44. Position counts over the full window: 0: 1413, 1: 295, 2: 112, 3: 58, 4: 35. Through 2019-08-30: 0: 62, 1: 68, 2: 21, 3: 6, 4: 11.

| date | daily return | equity | positions | cash | ticker | role | shares | entry | entry px | exit | exit px | day open | day close | prior close | split on the hold | bad bar |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2019-03-08 | -33.48% | 5,619.89 | 2 | 4.85 | ALT | held at close | 1163 | 2019-03-08 | 3.630000 | 2019-03-11 | 2.850000 | 3.630000 | 2.880000 | 4.540000 | no | no |
|  |  |  |  |  | CRDF | held at close | 472 | 2019-03-08 | 8.900000 | 2019-03-11 | 5.320000 | 8.900000 | 4.800000 | 4.130000 | no | no |
| 2020-03-26 | -28.60% | 88.48 | 2 | 13.88 | CAPR | sold at open | 61 | 2020-03-25 | 1.620000 | 2020-03-26 | 1.440000 | 1.440000 | 1.420000 | 2.020000 | no | no |
|  |  |  |  |  | EGHT | held at close | 2 | 2020-03-26 | 16.620001 | 2020-03-27 | 16.010000 | 16.620001 | 16.600000 | 18.040001 | no | no |
|  |  |  |  |  | XWEL | held at close | 3 | 2020-03-26 | 13.200000 | 2020-03-27 | 16.200001 | 13.200000 | 13.800000 | 19.200001 | no | no |
| 2019-08-27 | -25.79% | 1,082.12 | 1 | 4.76 | FSLY | sold at open | 26 | 2019-08-26 | 26.860001 | 2019-08-27 | 28.040001 | 28.040001 | 29.410000 | 27.559999 | no | no |
|  |  |  |  |  | OPTT | sold at open | 398 | 2019-08-26 | 1.780000 | 2019-08-27 | 1.830000 | 1.830000 | 1.730000 | 1.850000 | no | no |
|  |  |  |  |  | AIFC | held at close | 268 | 2019-08-27 | 5.400000 | 2019-08-28 | 4.100000 | 5.400000 | 4.020000 | 6.050000 | no | no |
| 2019-08-12 | -25.24% | 1,644.73 | 1 | 0.23 | AMPY | sold at open | 429 | 2019-08-09 | 4.510000 | 2019-08-12 | 4.510000 | 4.510000 | 5.120000 | 5.120000 | no | no |
|  |  |  |  |  | LOMA | held at close | 299 | 2019-08-12 | 6.450000 | 2019-08-13 | 5.870000 | 6.450000 | 5.500000 | 12.880000 | no | no |
| 2019-08-29 | -25.17% | 787.44 | 1 | 3.60 | SVM | sold at open | 235 | 2019-08-28 | 4.660000 | 2019-08-29 | 4.480000 | 4.480000 | 4.260000 | 4.470000 | no | no |
|  |  |  |  |  | SY | held at close | 71 | 2019-08-29 | 14.730000 | 2019-08-30 | 11.740000 | 14.730000 | 11.040000 | 16.600000 | no | no |
| 2019-04-11 | -24.46% | 3,334.29 | 2 | 0.79 | AACG | sold at open | 1146 | 2019-04-10 | 2.570000 | 2019-04-11 | 3.000000 | 3.000000 | 3.050000 | 3.850000 | no | no |
|  |  |  |  |  | AACG | held at close | 570 | 2019-04-11 | 3.000000 | 2019-04-12 | 2.860000 | 3.000000 | 3.050000 | 3.850000 | no | no |
|  |  |  |  |  | MARA | held at close | 500 | 2019-04-11 | 3.400000 | 2019-04-12 | 3.250000 | 3.400000 | 3.190000 | 3.860000 | no | no |
| 2020-01-28 | -23.51% | 224.89 | 1 | 2.59 | APT | sold at open | 11 | 2020-01-27 | 7.800000 | 2020-01-28 | 7.160000 | 7.160000 | 5.700000 | 7.700000 | no | no |
|  |  |  |  |  | VIR | sold at open | 3 | 2020-01-27 | 25.930000 | 2020-01-28 | 24.049999 | 24.049999 | 21.309999 | 24.775000 | no | no |
|  |  |  |  |  | VRDN | sold at open | 5 | 2020-01-27 | 16.200001 | 2020-01-28 | 22.049999 | 22.049999 | 23.700001 | 21.900000 | no | no |
|  |  |  |  |  | APT | held at close | 39 | 2020-01-28 | 7.160000 | 2020-01-29 | 5.850000 | 7.160000 | 5.700000 | 7.700000 | no | no |
| 2021-06-22 | -19.45% | 6.36 | 1 | 0.47 | GBR | held at close | 1 | 2021-06-22 | 7.350000 | 2021-06-23 | 6.050000 | 7.350000 | 5.890000 | 9.400000 | no | no |
| 2020-03-09 | -18.64% | 155.74 | 1 | 2.56 | ONCY | sold at open | 42 | 2020-03-06 | 2.620000 | 2020-03-09 | 2.070000 | 2.070000 | 1.930000 | 2.290000 | no | no |
|  |  |  |  |  | OPK | sold at open | 40 | 2020-03-06 | 2.790000 | 2020-03-09 | 2.840000 | 2.840000 | 2.220000 | 2.320000 | no | no |
|  |  |  |  |  | OPK | held at close | 69 | 2020-03-09 | 2.840000 | 2020-03-10 | 1.930000 | 2.840000 | 2.220000 | 2.320000 | no | no |
| 2020-02-11 | -18.41% | 100.58 | 1 | 14.58 | COLL | sold at open | 5 | 2020-02-10 | 24.000000 | 2020-02-11 | 21.600000 | 21.600000 | 22.209999 | 21.620001 | no | no |
|  |  |  |  |  | DAO | held at close | 4 | 2020-02-11 | 26.600000 | 2020-02-12 | 22.100000 | 26.600000 | 21.500000 | 29.500000 | no | no |

Dollar contributions of those rows (sold names at the open gap minus the sell fee, new buys at close minus entry minus the buy fee, carried names at the close-to-prior-close move) match the published equity change within $0.0000 on these ten days.

### longhist_zero_candle_h2

Maximum end-of-day positions 3. First close under $1,000 is 2023-10-23 at $965.92. Position counts over the full window: 0: 1676, 1: 220, 2: 13, 3: 4. Through 2019-08-30: 0: 157, 1: 10, 2: 1.

| date | daily return | equity | positions | cash | ticker | role | shares | entry | entry px | exit | exit px | day open | day close | prior close | split on the hold | bad bar |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2022-09-16 | -48.92% | 3,099.97 | 1 | 2.34 | SJ | held at close | 1009 | 2022-09-16 | 6.000000 | 2022-09-20 | 2.880000 | 6.000000 | 3.070000 | 5.000000 | no | no |
| 2021-10-29 | -38.85% | 6,846.35 | 1 | 16.80 | HUDI | held at close | 395 | 2021-10-28 | 22.040001 | 2021-11-01 | 15.560000 | 22.840000 | 17.290001 | 28.299999 | no | no |
| 2024-03-07 | -35.56% | 1,882.24 | 1 | 6.74 | AXG | held at close | 62 | 2024-03-06 | 21.799999 | 2024-03-08 | 32.480000 | 31.420000 | 30.250000 | 47.000000 | no | no |
| 2019-07-31 | -29.23% | 9,524.19 | 1 | 4.19 | SLS | held at close | 1120 | 2019-07-31 | 12.000000 | 2019-08-02 | 8.000000 | 12.000000 | 8.500000 | 10.000000 | no | no |
| 2025-06-12 | -28.24% | 402.18 | 1 | 1.85 | VTAK | held at close | 49 | 2025-06-11 | 13.490000 | 2025-06-13 | 7.030000 | 8.360000 | 8.170000 | 11.400000 | no | no |
| 2023-10-20 | -26.96% | 1,138.22 | 1 | 0.70 | OTLK | held at close | 118 | 2023-10-19 | 10.200000 | 2023-10-23 | 8.200000 | 12.420000 | 9.640000 | 13.200000 | no | no |
| 2022-07-06 | -24.50% | 5,465.32 | 1 | 4.12 | YSG | held at close | 666 | 2022-07-06 | 10.850000 | 2022-07-08 | 9.050000 | 10.850000 | 8.200000 | 10.000000 | no | no |
| 2024-10-29 | -24.40% | 760.41 | 1 | 0.41 | STKE | held at close | 76 | 2024-10-29 | 13.200000 | 2024-10-31 | 8.320000 | 13.200000 | 10.000000 | 11.680000 | no | no |
| 2026-03-31 | -24.39% | 118.47 | 1 | 0.17 | SLND | held at close | 91 | 2026-03-31 | 1.700000 | 2026-04-02 | 1.350000 | 1.700000 | 1.300000 | 1.800000 | no | no |
| 2024-03-13 | -21.72% | 1,579.90 | 1 | 1.92 | EONR | held at close | 514 | 2024-03-13 | 3.910000 | 2024-03-15 | 2.810000 | 3.910000 | 3.070000 | 4.468000 | no | no |

Dollar contributions of those rows (sold names at the open gap minus the sell fee, new buys at close minus entry minus the buy fee, carried names at the close-to-prior-close move) match the published equity change within $0.0000 on these ten days.

### Shared crash days

2019-03-08 is in the worst ten for `longhist_break10_h2` (-33.52%), `longhist_rvol_lg_h1` (-27.50%), `longhist_break10_h1` (-33.48%).
2019-04-11 is in the worst ten for `longhist_rvol_lg_h1` (-15.74%), `longhist_break10_h1` (-24.46%).
2020-03-26 is in the worst ten for `longhist_break10_h2` (-29.10%), `longhist_break10_h1` (-28.60%).

The names on those dates are in the tables above. A name bought by two gates is one cached open and one cached close, marked on each book.

On 2019-03-08 the shared buys are ALT (open 3.63, close 2.88, prior close 4.54) and CRDF (open 8.90, close 4.80, prior close 4.13). CRDF's open sits inside that day's high 9.65 and low 4.64, and the bar's volume is 28.6 million shares. On 2020-03-26 the shared name is CAPR: prior close 2.02, open 1.44, close 1.42. Those holds contain no split.

Two rvol holds have a wide open that still sits inside the high-low. UONEK on 2020-06-17 opened at 68.00 (high 68.40, low 23.80, close 26.50, volume 9.0 million). YHGJ on 2020-06-24 opened at 53.40 (high 59.70, low 32.40, close 33.00, volume 4.9 million). Yahoo records no split on either hold.

On every name in these tables, the ledger entry equals the cached open on the entry date and the ledger exit equals the cached open on the exit date. No structural bad bar (non-positive price, high below low, open or close outside the high-low, negative volume) is on the entry bar, that worst day, or the exit bar.

## 6. Flat 15bp equity series

The published `daily_returns.parquet` is the Futubull path. `daily_returns_15bp.parquet` is the same trades and the same share counts. The only change is the fee. A buy debits `shares * entry`. A sell credits `shares * exit - 0.0015 * shares * entry`. The 15bp charge is once, on the exit, which is the closed-trade definition in PREREG. An open lot at the window end is marked at the close and is not charged, because it is not a closed trade. The Futubull replay of the same ledger is compared with the published equity. A max absolute gap under one cent means the 15bp path is on the recorded book.

Largest Futubull replay gap across 84 books: part1 longhist_break10_h2 $0.000000.

Part 1 finals, both fee models, from a $10,000 start.

| rule | Futubull ending equity | Futubull return | 15bp ending equity | 15bp return |
| --- | --- | --- | --- | --- |
| longhist_break10_h2 | 3.85 | -99.96% | 1,092.66 | -89.07% |
| longhist_rvol_lg_h1 | 4.18 | -99.96% | 6,013.66 | -39.86% |
| longhist_break10_h1 | 1.32 | -99.99% | 2,004.14 | -79.96% |
| longhist_zero_candle_h2 | 118.62 | -98.81% | 1,117.53 | -88.82% |

Top 5 Part 2 rules by published Part 2 test Futubull ending equity. This is a sort of the books already scored. It is not a new pass rule and it does not change the frozen file. All 40 test books failed the addendum.

| rule | Futubull ending equity | Futubull return | 15bp ending equity | 15bp return |
| --- | --- | --- | --- | --- |
| longhist2_zero_px_h1_n2_candle_score | 2,836.87 | -71.63% | 4,194.30 | -58.06% |
| longhist2_zero_px_h1_n4_hot_score | 2,616.72 | -73.83% | 3,971.53 | -60.28% |
| longhist2_zero_px_h1_n4_candle_score | 2,609.03 | -73.91% | 3,963.53 | -60.36% |
| longhist2_zero_px_h1_n2_hot_score | 2,517.02 | -74.83% | 3,864.54 | -61.35% |
| longhist2_zero_px_h2_n2_candle_score | 1,393.01 | -86.07% | 2,377.93 | -76.22% |

Every rule's two endings are in `equity_15bp_totals.json`, next to the daily 15bp series.

## 7. All-panel plumbing control

Outside the tally. Each session buys every name on that day's stand-in panel, equal-weight, at the open, and sells at the close. The panel is `build_panel` on the cached bars, XNYS sessions 2019-01-02 through 2026-08-12, the same fee function, and the same whole-share leftover loop. No gate and no `top_n`. This is not a Part 1 result and not a Part 2 result.

Sessions 1913. Panel size mean 13.42, min 0, max 60. Names filled by the compounded Futubull account, mean 1.23. Sessions with an empty panel: 14. Missing closes: 0.

While compounded Futubull equity is still above $5,000 (59 sessions, mean panel 4.05), filled names equal panel names on every session (shortfall sessions: 0). After the account is a few tens of dollars, an open above the leftover budget is skipped, and the full-window mean fill falls to 1.23. The fixed $10,000 ticket is the path that keeps funding the panel.

On the fixed ticket the 15bp charge averages $14.78 a day, which is 14.78 bp of $10,000 against a 15 bp schedule. That is 98.5% of the ticket deployed. The rest is the whole-share residual and any name whose open exceeds its slice.

Compounded Futubull, start $10,000, reinvest what is left, fee on the buy and on the sell: ending equity $9.12 (-99.91%). Compounded 15bp uses the same panel and the same whole-share split, with no fee on the buy and `0.0015 * shares * open` once at the close: ending equity $103.05 (-98.97%).

Fixed $10,000 notional per day, not compounded, same buy loop against a fresh $10,000. Sum of price P&L $-32,824.34. Sum after Futubull $-178,598.07. Sum after 15bp $-61,096.35. Per day that is $-17.16 gross, $-93.36 Futubull, $-31.94 at 15bp, on a $10,000 ticket (-0.17% gross, -0.93% Futubull, -0.32% at 15bp).

Worked session 2019-01-03, compounded account cash before the buys $10,522.34.

| ticker | shares | open | close | buy fee | sell fee | 15bp | price P&L |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SBS | 1700 | 2.062000 | 1.932000 | 21.9300 | 22.2385 | 5.2581 | -221.0000 |
| ATHE | 194 | 18.000000 | 17.500000 | 2.5720 | 2.6314 | 5.2380 | -97.0000 |
| SMPL | 195 | 17.860001 | 19.920000 | 2.5750 | 2.6384 | 5.2241 | 401.6999 |

Price P&L is `shares * (close - open)`. Futubull P&L subtracts the buy fee and the sell fee. 15bp P&L subtracts `0.0015 * shares * open` once.

### Recorded Part 1 fills against the look list

The same panel, the Part 1 gates, and the leftover buy loop, walked in calendar order. Expected shares are compared with `trades_part1.parquet`. A mismatch would be a plumbing bug and would not be fixed in this pull request.

`longhist_break10_h2`: look-list length counts {"0": 275, "1": 392, "2": 314, "3": 214, "4": 718}. Recorded buy counts {"0": 1487, "1": 327, "2": 70, "3": 18, "4": 11}. Mismatched sessions: 0.

`longhist_rvol_lg_h1`: look-list length counts {"0": 86, "1": 180, "2": 207, "3": 208, "4": 1232}. Recorded buy counts {"0": 1121, "1": 362, "2": 137, "3": 92, "4": 201}. Mismatched sessions: 0.

`longhist_break10_h1`: look-list length counts {"0": 275, "1": 392, "2": 314, "3": 214, "4": 718}. Recorded buy counts {"0": 1413, "1": 295, "2": 112, "3": 58, "4": 35}. Mismatched sessions: 0.

`longhist_zero_candle_h2`: look-list length counts {"0": 1777, "1": 130, "2": 5, "3": 1}. Recorded buy counts {"0": 1791, "1": 116, "2": 5, "3": 1}. Mismatched sessions: 0.

Every Part 1 session matches the preregistered look list and the leftover share loop. End-of-day counts of 1–3 are the gate and the cash, on a book whose cap is `top_n` (4 names, or 8 when a hold of 2 still carries the prior cohort). The panel cap of 60 is the candidate list those gates read. No plumbing bug showed up in that fill audit, in the Futubull replay of the saved ledgers, or in the open-to-close prices. Nothing here is corrected in this pull request.
