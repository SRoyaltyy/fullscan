# Part 1

Window 2019-01-01 through 2026-08-12. Year pass is 4 of 2019–2024. Futubull return is ending equity over $10,000, including open marks. Removed return is the rerun with the best closed-P&L ticker ineligible.

| rule | 6.1 mean>0 Holm | years | best removed | >=30/yr | study | trades | buy fills | Futubull return | removed return | closed P&L | removed closed P&L |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| longhist_break10_h2 | fail | fail | fail | pass | fail | 565 | 565 | -99.96% | -99.98% | -9,996.15 | -9,998.15 |
| longhist_rvol_lg_h1 | fail | fail | fail | pass | fail | 1716 | 1716 | -99.96% | -99.96% | -9,995.82 | -9,996.21 |
| longhist_break10_h1 | fail | fail | fail | pass | fail | 833 | 833 | -99.99% | -99.99% | -9,998.68 | -9,998.75 |
| longhist_zero_candle_h2 | fail | fail | fail | fail | fail | 129 | 129 | -98.81% | -99.04% | -9,881.38 | -9,904.23 |

RANDOM4 mean ending-equity return: -99.96%.

IWM 2019-01-02 → 2026-08-12: Futubull return 127.86%, 15bp return 127.76%.

## Per rule

### longhist_break10_h2

Cluster mean -0.018981, t -1.9557859954733987, p 0.9744274310372268, Holm p 1, entry days 426.

Fires/year 74.43. Buys by year {"2019": 194, "2020": 181, "2021": 35, "2022": 27, "2023": 45, "2024": 50, "2025": 21, "2026": 12}.

Ending equity 3.85. 15bp closed P&L on the same shares -8,907.34. Win rate 33.63%. Best stock INSM. RANDOM4 beaten 58.50%.

Year closed P&L {"2019": -9864.97, "2020": -123.91, "2021": -7.2, "2022": 0.5, "2023": -0.2, "2024": 0.55, "2025": -1.17, "2026": 0.24}.

Luck test 1-share mean -0.028857896310360207, p_rule 0.6866313368663134.

### longhist_rvol_lg_h1

Cluster mean -0.009475, t -1.9222539372784302, p 0.9725336534038862, Holm p 1, entry days 792.

Fires/year 226.05. Buys by year {"2019": 476, "2020": 812, "2021": 177, "2022": 55, "2023": 64, "2024": 73, "2025": 29, "2026": 30}.

Ending equity 4.18. 15bp closed P&L on the same shares -3,986.34. Win rate 37.88%. Best stock FCEL. RANDOM4 beaten 84.40%.

Year closed P&L {"2019": -6571.78, "2020": -3391.28, "2021": -28.47, "2022": -2.12, "2023": -1.7, "2024": 1.99, "2025": -1.75, "2026": -0.69}.

Luck test 1-share mean -0.02281114635687418, p_rule 0.4784521547845215.

### longhist_break10_h1

Cluster mean -0.019200, t -3.061139223855703, p 0.9988381014085744, Holm p 1, entry days 500.

Fires/year 109.73. Buys by year {"2019": 273, "2020": 386, "2021": 132, "2022": 21, "2023": 9, "2024": 7, "2025": 2, "2026": 3}.

Ending equity 1.32. 15bp closed P&L on the same shares -7,995.86. Win rate 35.41%. Best stock CRMD. RANDOM4 beaten 0.00%.

Year closed P&L {"2019": -9645.77, "2020": -332.97, "2021": -16.77, "2022": -1.64, "2023": -1.16, "2024": 0.39, "2025": -0.45, "2026": -0.31}.

Luck test 1-share mean -0.025351136451679697, p_rule 0.8355164483551645.

### longhist_zero_candle_h2

Cluster mean -0.023642, t -1.4921760604385657, p 0.9308721447600862, Holm p 1, entry days 122.

Fires/year 16.99. Buys by year {"2019": 9, "2020": 10, "2021": 13, "2022": 15, "2023": 18, "2024": 16, "2025": 26, "2026": 22}.

Ending equity 118.62. 15bp closed P&L on the same shares -8,882.47. Win rate 38.76%. Best stock VECO. RANDOM4 beaten 100.00%.

Year closed P&L {"2019": -1764.33, "2020": -2616.76, "2021": 1087.06, "2022": -4349.66, "2023": -959.34, "2024": -896.86, "2025": -333.07, "2026": -48.43}.

Luck test 1-share mean -0.038578633495551094, p_rule 0.791920807919208.

Best-of-N luck p: 0.8739126087391261.
