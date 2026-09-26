# Rule 20 restatement, forward window 2026-09-14 through 2026-09-25

This file is a new reading. It does not replace `FORWARD.json` or `REPORT.md`.

The committed without-best-stock, without-CYPH, without-GLND, and without-INDP figures for this window started the book at $10,000. The continuous book already had whatever equity the 2026-09-11 close had reached. Keep-held remains the Futubull figure. The window return itself (the compounded daily ratios) was already measured from that prior equity, so the compound column is unchanged. Only the dollar drops were wrong.

Who carries forward is unchanged. A reject still uses the trade count and the joint of win rate and up-day share. Rule 20 is not that test.

| id | status | compound | ex-best corrected | old ex-best | CYPH out | GLND out | INDP out | best |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `union_hot_score_h3__w0` | rejected | 5.77% | -0.81% | 19.43% | 6.37% | -0.81% | 3.20% | GLND |
| `union_hot_n4_h1__w0` | not_rejected | 36.02% | 5.62% | 46.72% | 39.24% | 5.62% | 29.09% | GLND |
| `union_hot_n4_holdup__w0` | not_rejected | 38.82% | 11.25% | 72.01% | 38.85% | 11.25% | 29.89% | GLND |
| `union_candle_score_h3__w0` | rejected | 2.71% | -0.52% | 0.75% | 2.71% | 2.71% | -0.52% | INDP |
| `union_cond_h3__w0` | rejected | -5.06% | -7.13% | -3.36% | -5.33% | -5.06% | -5.06% | FIVN |
| `union_ret_5_h3__w0` | rejected | 3.12% | 0.93% | 21.82% | 3.60% | 0.93% | 2.73% | GLND |
| `union_w_hot_cond_h3__w0` | rejected | 3.84% | -1.16% | 13.41% | 4.64% | -1.16% | 2.04% | GLND |
| `union_w_hot_candle_h3__w0` | rejected | 9.03% | -0.72% | 14.02% | 9.47% | -0.72% | 6.77% | GLND |
| `union_hot_n12_h1__w0` | rejected | 2.13% | -7.89% | -11.04% | 3.23% | -7.89% | -0.70% | GLND |
| `union_hot_n4_h3__w0` | unproven | 29.34% | 5.71% | 21.68% | 30.60% | 5.71% | 25.22% | GLND |
| `union_hot_score_h1__w0` | rejected | 10.72% | -4.21% | 1.99% | 12.37% | -4.21% | 6.46% | GLND |
| `union_hot_n4_h5__w0` | unproven | 13.46% | 2.91% | 25.00% | 13.46% | 13.44% | 13.40% | BNC |
| `union_hot_score_h3_time__w0` | rejected | 6.72% | -0.75% | 17.88% | 6.94% | -0.75% | 3.95% | GLND |
| `union_hot_n4_h1_time__w0` | rejected | 31.50% | 4.23% | 40.57% | 34.69% | 4.23% | 24.54% | GLND |
| `union_hot_score_h3_exitalarm__w0` | rejected | 6.64% | 0.12% | 9.85% | 7.33% | 0.12% | 4.08% | GLND |
| `union_hot_score_h3_holdup__w0` | rejected | 5.77% | -0.81% | 19.43% | 6.37% | -0.81% | 3.20% | GLND |
| `union_hot_score_h3_green__w0` | rejected | -2.29% | -4.61% | 4.70% | -1.83% | -4.61% | -2.91% | GLND |
| `union_hot_n4_h1_green__w0` | unproven | 15.13% | -4.73% | 10.30% | 18.36% | -4.73% | 14.31% | GLND |
| `union_hot_score_h3_nonews__w0` | rejected | 5.41% | -1.84% | 16.43% | 5.92% | -1.84% | 2.77% | GLND |
| `union_hot_n4_h1_nonews__w0` | not_rejected | 35.47% | 5.04% | 52.07% | 38.70% | 5.04% | 28.54% | GLND |

Excel's corrected without-best-stock figures, which this reading matches to one decimal percent: `union_hot_n4_h1_time__w0` +4.2%, `union_hot_n4_h1__w0` +5.6%, `union_hot_n4_holdup__w0` +11.2%, `union_hot_n4_h1_nonews__w0` +5.0%.

## Holdup does nothing on the h3 rows

`union_hot_score_h3` and `union_hot_score_h3_holdup` write the same daily series under both weather settings. The committed return files have no differing session.

- `union_hot_score_h3__w0=union_hot_score_h3_holdup__w0`: 31 sessions, 0 differences
- `union_hot_score_h3__w1=union_hot_score_h3_holdup__w1`: 31 sessions, 0 differences

Holdup, on an up morning that is not a hard-red sit, sets a new lot's minimum hold to the greater of the recipe hold and 2 sessions. The h3 recipe already holds for 3 sessions, so that floor stays 3. No lot sells on a different day. The weather gate only sits new buys on a hard-red morning. It does not change this floor. Holdup does change an h1 recipe, where the floor can rise from 1 session to 2.

## `union_hot_n4_holdup__w0` carries because it was not rejected

`union_hot_n4_holdup__w0` cleared 2 of the 3 Monday tuning starts. It is not a passer, and it has no rank key. It is on the frozen top 20, so the forward window checked it. The window left it not rejected (joint 53.33% on 30 trades). It carries from 2026-09-28 for that reason.
