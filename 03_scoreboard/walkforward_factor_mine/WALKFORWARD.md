# Walk-forward factor-mine discovery

**KEEP** the 50/50 sleeve as +EV vs a matched random control. **KILL** the published `combo_sh_5050_shared` **+35.9%** as a live number.

KEEP the 50/50 sleeve as +EV-vs-random on this thin tape (3/4 hidden windows green, mean Book% +4.41% vs random -3.21%). KILL the +35.9% full-sample print as a forecast — that number is the in-sample butterfly after the cutoff has already seen almost every session. Walk-forward OOS is a few percent per 3–4 session window, not +35%.

IS discovery process (re-pick best long + best short each cutoff, freeze a 50/50) OOS mean Book% +0.39% — barely +EV OOS; chasing the IS winner is weaker than freezing the a-priori 50/50.

## Method

The published [FACTOR_MINE.md](../FACTOR_MINE.md) board searched **161** leak-free 09:30 single recipes (~235 once the combo books are counted) on the **same** sessions it then ranked. That is multiple-testing contaminated.

This board:

1. Uses the existing leak-free 09:30 panel `2026-08-13 → 2026-09-11` (21 sessions).
2. At each cutoff, scores the recipe grid with the audited `simulate_book` cash ledger on sessions **≤ cutoff only**.
3. Freezes the best-IS long, best-IS short, and a 50/50 shared combo of those two.
4. Wakes a **fresh $10k** book on the hidden window after the cutoff (never used in selection). Open lots at the cutoff do not carry forward.
5. Always scores controls: (A) `union_hot_n4_h1` (B) `short_news_r_h3` (C) `combo_sh_5050_shared` (D) random L/S matched to C's daily head-count (E) market-neutralized residuals (daily Book% − exposure × median panel open→close).

Fills stay the published cash rules: whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit. Research only — does **not** change live `flatten_robust` or the cash book.

Cutoffs start `2026-08-20`, step 4 sessions, hidden window 4 sessions (last fold may be shorter).

`docs/HOW_IT_WORKS*` was not in the repo; method follows `src/factor_mine.py` + `src/factor_mine_book.py`.

## Pooled hidden windows

| Sleeve | Role | Folds green | Mean Book% | Hit rate | n sess | $ P&L |
|---|---|---:|---:|---:|---:|---:|
| `control_combo_sh` | frozen 50/50 (the +35.9 claim) | 3/4 | +4.41 | 53% | 15 | +1765.17 |
| `control_hot4` | A · hot-4 long | 1/4 | +3.80 | 40% | 15 | +1520.29 |
| `control_news_red` | B · news-red short | 2/4 | +1.58 | 40% | 15 | +632.06 |
| `control_random` | D · random L/S matched | 1/4 | -3.21 | 27% | 15 | -1284.71 |
| `discovered_5050` | IS-picked 50/50 (process) | 2/4 | +0.39 | 47% | 15 | +157.66 |
| `discovered_top` | IS-picked top single | 2/4 | -0.76 | 47% | 15 | -303.74 |
| `control_combo_sh_mktneut` | E · 50/50 mkt-neutralized | 3/4 | +4.08 | 53% | 15 | +1633.34 |
| `control_hot4_mktneut` | E · hot-4 mkt-neutralized | 2/4 | +4.15 | 40% | 15 | +1658.17 |
| `control_news_red_mktneut` | E · news-red mkt-neutralized | 2/4 | +1.27 | 53% | 15 | +508.75 |
| `discovered_5050_mktneut` | E · IS 50/50 mkt-neutralized | 2/4 | +0.65 | 40% | 15 | +260.26 |

Frozen `combo_sh_5050_shared` mean OOS Book% +4.41% vs random -3.21%.

## Folds

| Cutoff | IS n | Hidden window | IS combo Book% | OOS combo | OOS hot-4 | OOS news-red | OOS random | IS pick (L / S) | OOS IS-50/50 |
|---|---:|---|---:|---:|---:|---:|---:|---|---:|
| `2026-08-20` | 6 | 2026-08-21 → 2026-08-26 | +3.27 | +12.51 | +20.79 | -0.67 | +2.85 | union_e_green_h3 / short_news_r_h3 | +2.08 |
| `2026-08-26` | 10 | 2026-08-27 → 2026-09-01 | +20.65 | +3.69 | -1.52 | +3.74 | -5.92 | union_e_fresh_h3 / short_news_r_h3 | -2.69 |
| `2026-09-01` | 14 | 2026-09-02 → 2026-09-08 | +31.24 | +2.76 | -2.85 | +3.95 | -6.09 | union_hot_n4_h1 / short_news_r_h3 | +2.76 |
| `2026-09-08` | 18 | 2026-09-09 → 2026-09-11 | +35.53 | -1.30 | -1.21 | -0.69 | -3.69 | union_e_fresh_h3 / short_news_r_h3 | -0.56 |

## Per-fold OOS detail

### Cutoff `2026-08-20` (IS 6 · hidden 4)

IS selected **top** `union_e_green_h3` (+38.01%), **long** `union_e_green_h3` (+38.01%), **short** `short_news_r_h3` (+3.86%). Scored 161 recipes, 161 usable.

| Name | Role | Book% | Hit | n | Trades | Win% | Audit |
|---|---|---:|---:|---:|---:|---:|---|
| `union_e_green_h3` | discovered_top | +2.17 | 75% | 4 | 15 | 100% | PASS |
| `short_news_r_h3` | discovered_short | -0.67 | 25% | 4 | 19 | 67% | PASS |
| `discovered_5050_short_news_r_h3+union_e_green_h3` | discovered_5050 | +2.08 | 75% | 4 | 31 | 78% | PASS |
| `union_hot_n4_h1` | control_hot4 | +20.79 | 75% | 4 | 18 | 86% | PASS |
| `short_news_r_h3` | control_news_red | -0.67 | 25% | 4 | 19 | 67% | PASS |
| `combo_sh_5050_shared` | control_combo_sh | +12.51 | 75% | 4 | 35 | 83% | PASS |
| `random_ls_matched` | control_random | +2.85 | 50% | 4 | 40 | 36% | PASS |
| `discovered_5050_short_news_r_h3+union_e_green_h3_mktneut` | neutralized | +1.92 | 75% | 4 | 31 | 78% | PASS |
| `union_hot_n4_h1_mktneut` | neutralized | +19.60 | 75% | 4 | 18 | 86% | PASS |
| `short_news_r_h3_mktneut` | neutralized | -0.10 | 50% | 4 | 19 | 67% | PASS |
| `combo_sh_5050_shared_mktneut` | neutralized | +12.20 | 75% | 4 | 35 | 83% | PASS |

### Cutoff `2026-08-26` (IS 10 · hidden 4)

IS selected **top** `union_e_fresh_h3` (+24.33%), **long** `union_e_fresh_h3` (+24.33%), **short** `short_news_r_h3` (+3.01%). Scored 161 recipes, 161 usable.

| Name | Role | Book% | Hit | n | Trades | Win% | Audit |
|---|---|---:|---:|---:|---:|---:|---|
| `union_e_fresh_h3` | discovered_top | -2.77 | 50% | 4 | 5 | 0% | PASS |
| `short_news_r_h3` | discovered_short | +3.74 | 75% | 4 | 2 | — | PASS |
| `discovered_5050_short_news_r_h3+union_e_fresh_h3` | discovered_5050 | -2.69 | 50% | 4 | 5 | 0% | PASS |
| `union_hot_n4_h1` | control_hot4 | -1.52 | 50% | 4 | 16 | 62% | PASS |
| `short_news_r_h3` | control_news_red | +3.74 | 75% | 4 | 2 | — | PASS |
| `combo_sh_5050_shared` | control_combo_sh | +3.69 | 75% | 4 | 18 | 62% | PASS |
| `random_ls_matched` | control_random | -5.92 | 25% | 4 | 18 | 12% | PASS |
| `discovered_5050_short_news_r_h3+union_e_fresh_h3_mktneut` | neutralized | -0.58 | 50% | 4 | 5 | 0% | PASS |
| `union_hot_n4_h1_mktneut` | neutralized | +0.89 | 50% | 4 | 16 | 62% | PASS |
| `short_news_r_h3_mktneut` | neutralized | +2.58 | 75% | 4 | 2 | — | PASS |
| `combo_sh_5050_shared_mktneut` | neutralized | +3.61 | 100% | 4 | 18 | 62% | PASS |

### Cutoff `2026-09-01` (IS 14 · hidden 4)

IS selected **top** `union_hot_n4_h1` (+20.80%), **long** `union_hot_n4_h1` (+20.80%), **short** `short_news_r_h3` (+9.84%). Scored 161 recipes, 161 usable.

| Name | Role | Book% | Hit | n | Trades | Win% | Audit |
|---|---|---:|---:|---:|---:|---:|---|
| `union_hot_n4_h1` | discovered_top | -2.85 | 25% | 4 | 14 | 29% | PASS |
| `short_news_r_h3` | discovered_short | +3.95 | 50% | 4 | 4 | — | PASS |
| `discovered_5050_short_news_r_h3+union_hot_n4_h1` | discovered_5050 | +2.76 | 50% | 4 | 18 | 29% | PASS |
| `union_hot_n4_h1` | control_hot4 | -2.85 | 25% | 4 | 14 | 29% | PASS |
| `short_news_r_h3` | control_news_red | +3.95 | 50% | 4 | 4 | — | PASS |
| `combo_sh_5050_shared` | control_combo_sh | +2.76 | 50% | 4 | 18 | 29% | PASS |
| `random_ls_matched` | control_random | -6.09 | 25% | 4 | 20 | 57% | PASS |
| `discovered_5050_short_news_r_h3+union_hot_n4_h1_mktneut` | neutralized | +1.83 | 25% | 4 | 18 | 29% | PASS |
| `union_hot_n4_h1_mktneut` | neutralized | -2.69 | 25% | 4 | 14 | 29% | PASS |
| `short_news_r_h3_mktneut` | neutralized | +3.30 | 75% | 4 | 4 | — | PASS |
| `combo_sh_5050_shared_mktneut` | neutralized | +1.83 | 25% | 4 | 18 | 29% | PASS |

### Cutoff `2026-09-08` (IS 18 · hidden 3)

IS selected **top** `union_e_fresh_h3` (+22.78%), **long** `union_e_fresh_h3` (+22.78%), **short** `short_news_r_h3` (+14.68%). Scored 161 recipes, 161 usable.

| Name | Role | Book% | Hit | n | Trades | Win% | Audit |
|---|---|---:|---:|---:|---:|---:|---|
| `union_e_fresh_h3` | discovered_top | +0.41 | 33% | 3 | 8 | — | PASS |
| `short_news_r_h3` | discovered_short | -0.69 | 0% | 3 | 4 | — | PASS |
| `discovered_5050_short_news_r_h3+union_e_fresh_h3` | discovered_5050 | -0.56 | 0% | 3 | 12 | — | PASS |
| `union_hot_n4_h1` | control_hot4 | -1.21 | 0% | 3 | 4 | — | PASS |
| `short_news_r_h3` | control_news_red | -0.69 | 0% | 3 | 4 | — | PASS |
| `combo_sh_5050_shared` | control_combo_sh | -1.30 | 0% | 3 | 8 | — | PASS |
| `random_ls_matched` | control_random | -3.69 | 0% | 3 | 8 | — | PASS |
| `discovered_5050_short_news_r_h3+union_e_fresh_h3_mktneut` | neutralized | -0.56 | 0% | 3 | 12 | — | PASS |
| `union_hot_n4_h1_mktneut` | neutralized | -1.21 | 0% | 3 | 4 | — | PASS |
| `short_news_r_h3_mktneut` | neutralized | -0.69 | 0% | 3 | 4 | — | PASS |
| `combo_sh_5050_shared_mktneut` | neutralized | -1.30 | 0% | 3 | 8 | — | PASS |

## KEEP / KILL

**KEEP** the sleeve. **KILL** the +35.9% print.

KEEP the 50/50 sleeve as +EV-vs-random on this thin tape (3/4 hidden windows green, mean Book% +4.41% vs random -3.21%). KILL the +35.9% full-sample print as a forecast — that number is the in-sample butterfly after the cutoff has already seen almost every session. Walk-forward OOS is a few percent per 3–4 session window, not +35%.

Read the IS combo Book% column: it climbs toward +35% only as the cutoff eats the sample (the contamination the published board reported as a win). The hidden windows after each cutoff are the number that matters, and they are small.

This is a research scoreboard. It does not wire anything into `flatten_robust` and it does not change the live cash book.
