# Long-history study — addendum 1

- status: locked before any score. No Part 1 or Part 2 return has been computed.
- written: 2026-09-26
- does not edit: `research/longhist/PREREG.md` (fingerprint `0e519c4f8e3081571b79f23ef15f9946716ebeb46c354ea7e8c53470ec78394a`)
- fingerprint_sha256: 514e3fbe3a78c5ae736d908956bbe6f6bfd6fccd60890af16e79399a57db4589
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.

<!-- BEGIN COVERED -->

Nothing in the preregistration file changes. This addendum adds the Part 2 out-of-sample pass, confirms the pre-open filters, and records the delisted-symbol manifest. No strategy has been scored. There was no score file to delete.

## 1. Part 2 out-of-sample pass

Section 6.2 (positive in at least 4 of 2019–2024) applies to **Part 1 only**. It is not a Part 2 out-of-sample pass rule.

The Part 2 train filter in preregistration section 8 is unchanged: a winner is still chosen only on 2019-01-01..2023-12-31, and the frozen file is still committed before any test session is scored.

On the test window only (2024-01-01..2026-08-12), after Futubull fees, a Part 2 rule passes out of sample when all four of the following hold:

1. The sum of closed-trade P&L with an exit date in **2024** is > 0, and the sum of closed-trade P&L with an exit date from **2025-01-01 through 2026-08-12** is > 0. Both periods are required. 2025 and 2026-to-date are one period, not two.
2. Section 6.1 on the test window: the mean of the entry-day cluster returns is > 0, and the Holm-adjusted one-sided p-value is < 0.05, with N = 40 (every combination, not only the frozen winners).
3. Section 6.3 on the test window: the book rerun with the single best test-window stock removed has a closed-trade P&L sum > 0.
4. Section 6.4 on the test window: `n_buy_fills / (n_test_sessions / 252) ≥ 30`.

Flat 15bp stays a reported column. It is not one of these four.

## 2. Pre-open filters

Confirmed. The price filter, the 20-day dollar-volume mean, and prior-day relative volume use only bars with `date < D`, as written in preregistration lines 96-101 and 133-137.

- The price band is the close of the last bar dated before D.
- Dollar volume is the mean of `close * volume` over the last 20 of those same bars.
- Relative volume is `ohlc_ripper.from_bars` on those same bars (`volume[-1] / mean(volume[-20:])`).
- Session D contributes its official open to the gap test. Its high, low, close, and volume are not read for membership.

`research/longhist/membership.py` is that check and nothing else. `research/longhist/test_membership.py` builds a name that qualifies, then sets session D's close to 500 and its volume to 100,000,000 (enough to fail the $30 price band and the $40M dollar-volume cap if that bar were included). The capped membership list is unchanged. A second assertion moves the prior close to 80 and the name drops out, so the price filter is actually reading the pre-open bar.

## 3. Delisted and unchartable manifest

File: `research/longhist/tickers.json`

sha256: `40e5b09c5460d71d07947c62c1123d9cd43ec03cab6abcbbb3527acbe074e653`

Candidates are every `Ticker` in `data/exports/finviz_*.csv` on commit `9e3100853` that is not one of the 4,193 lines in `research/longhist/listed_common.txt`. There are **8,018** such symbols. A dot in a symbol is requested from Yahoo as a hyphen.

The chart v8 meta object on this date has `instrumentType` and does not have `quoteType` (the field named in the preregistration). `served` uses `instrumentType == EQUITY` and `currency == USD`. That is the same equity-versus-ETF cut, under the field Yahoo actually returns.

A symbol **charts** when the response has at least one daily bar whose New York session date is on or before 2026-08-12. It **does not chart** otherwise (HTTP 404, HTTP 400, or HTTP 200 with no such bar).

| count | meaning |
| --- | --- |
| 4,193 | `n_listed` (the current common-stock file; not re-probed here) |
| 8,018 | `n_delisted_candidates` |
| 7,400 | Yahoo charts (`n_yahoo_charts`) |
| 618 | Yahoo does not chart (`n_yahoo_does_not_chart`) |
| 2,138 | `n_delisted_served` (charts, EQUITY, USD) |
| 5,880 | `n_delisted_absent` (does not chart, or charts but is not EQUITY USD) |

Of the 7,400 that chart, 5,262 are ETFs (`instrumentType` ETF) and are absent under the equity rule. Of the 618 that do not chart, 281 are HTTP 404, 200 are HTTP 400, and 137 are HTTP 200 with no bar on or before the cutoff.

11 of the 2,138 served names are Finviz hyphen spellings of dotted symbols already in the listed file (`AKO-A`, `AKO-B`, `BF-A`, `BF-B`, `BRK-A`, `BRK-B`, `CRD-A`, `CRD-B`, `HEI-A`, `MOG-A`, `UHAL-B`). They are in this extra set because the strings differ. They are not delisted companies. The other 2,127 served names are not in the listed file under either spelling.

Thresholds are not changed by these counts.
