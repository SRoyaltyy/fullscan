# Theme Radar — OOS mine candidates (12 frozen non-headline KEEP rules)

**Author Theme Radar; submitted 2026-09-26 00:21 HKT. These rules were mined on 08-13..09-11, but I have seen their 09-14..09-24 results, so per James their 09-14..09-25 results are designed after the fact; only locked days from 09-28 count.**

All 12 are submitted exactly as frozen (no retune, no cherry-pick). Each was found as a short/fade signal. Each can be used two ways:
- **AVOID gate on longs (optional):** skip a long pick on the morning the rule fires.
- **Original short form:** short at the T+1 close, cover at the close h sessions later.

## Common settings
- **Clock:** Theme Radar Finviz after-close snapshot on day T, plus the gate on the T+1 morning. Entry at T+1 close, exit at close T+1+h.
- **"Top 20%"** = the frozen per-date rule: pandas qcut into 5 bins across that day's Theme Radar panel (levels(T) ∩ labels(T)), keep the top bin. No fixed number.
- **"Change vs T-k"** = value at T minus value k Theme Radar snapshots earlier. since_first = value at T minus value on 2026-08-06.
- **Earnings gate** (`univ_earn_today`) = fullscan `data/universe/<T+1>_membership.csv`, where `earn` is one of today / tomorrow / this_week. This is a Finviz Elite label, with no news input.
- **Excel gates** come from excel_bot `excel_clear_letter_panel.csv` for the T+1 date. `ER==1` is Excel's own avoid-ER open letter. Lag-Hammer means DF_lag1 contains "Hammer" and not "Inverted". Both are built from prior OHLC bars only.
- **Costs used:** 10bp round trip in-sample. The room standard from 09-25 is 15bp plus 0.3% borrow for shorts.

## Rules

| # | Plain-language AVOID gate (skip long when…) | Field / threshold / lag / hold | Data source | In-sample (08-13..09-11) n / hit / after-fee | Seen OOS 09-14..09-24 (not counted) | Eligible |
|---|---|---|---|---|---|---|
| 1 | Stock has earnings this week, and its Forward P/E is in the top 20% of the day | `lvl_Forward P/E` top quintile; lag = level at T; gate earn; hold 3d | TR snapshot T + fullscan membership | 331 / 65.9% / +2.20% | too thin (19, 26%) | YES |
| 2 | Earnings this week, and the rise in Forward P/E vs 3 snapshots ago is in the top 20% | `d\|T-3\|Forward P/E` top quintile; T-3; earn; 3d | TR snapshots + membership | 343 / 65.6% / +2.71% | too thin (17, 71%) | YES |
| 3 | Earnings this week, and the rise in Forward P/E vs 2 snapshots ago is in the top 20% | `d\|T-2\|Forward P/E` top quintile; T-2; earn; 3d | TR snapshots + membership | 343 / 62.7% / +1.83% | too thin (13, 69%) | YES |
| 4 | Earnings this week, and Forward P/E is higher than 3 snapshots ago | `d\|T-3\|Forward P/E` > 0; T-3; earn; 3d | TR snapshots + membership | 605 / 64.1% / +2.41% | PASS; the only one that survives 15bp + borrow (30, 63.3%, +0.87%) | YES |
| 5 | Earnings this week, and the rise in Market Cap vs 3 snapshots ago is in the top 20% | `d\|T-3\|Market Cap` top quintile; T-3; earn; 3d | TR snapshots + membership | 442 / 64.3% / +2.06% | too thin (29, 66%, −1.62%) | YES |
| 6 | Earnings this week, and the rise in Forward P/E since 08-06 is in the top 20% | `d\|since_first\|Forward P/E` top quintile; vs 08-06; earn; 3d | TR snapshots + membership | 323 / 65.3% / +2.57% | too thin (6) | YES |
| 7 | Analyst Recom number fell vs 3 snapshots ago (an upgrade), and Excel ER==1 | `d\|T-3\|Analyst Recom` < 0; T-3; gate ER==1; 3d | TR snapshots + excel_bot letter panel | 86 / 61.6% / +2.11% | PASS at 10bp; FAIL after borrow (−0.09%) | YES |
| 8 | Rise in Forward P/E vs 1 snapshot ago is in the top 20%, and yesterday was a Hammer candle | `d\|T-1\|Forward P/E` top quintile; T-1; lag-Hammer; hold 2d | TR snapshots + excel_bot panel | 153 / 58.8% / +0.76% | FAIL (45.8%) | YES |
| 9 | Same as #8, held 3 days | `d\|T-1\|Forward P/E` top quintile; T-1; lag-Hammer; 3d | TR snapshots + excel_bot panel | 153 / 62.1% / +0.81% | FAIL (47.5%) | YES |
| 10 | Forward P/E is higher than 2 snapshots ago, and yesterday was a Hammer | `d\|T-2\|Forward P/E` > 0; T-2; lag-Hammer; 2d | TR snapshots + excel_bot panel | 379 / 57.3% / +0.43% | PASS at 10bp; FAIL after borrow (49.1%) | YES |
| 11 | Rise in Forward P/E since 08-06 is in the top 20%, and yesterday was a Hammer | `d\|since_first\|Forward P/E` top quintile; vs 08-06; lag-Hammer; 2d | TR snapshots + excel_bot panel | 139 / 57.6% / +0.71% | FAIL (52.4%) | YES |
| 12 | Same as #11, held 3 days | `d\|since_first\|Forward P/E` top quintile; vs 08-06; lag-Hammer; 3d | TR snapshots + excel_bot panel | 139 / 61.2% / +0.90% | FAIL (46.5%) | YES |

## Proof that inputs were committed before 2026-09-14 09:30 ET (= 09-14 21:30 HKT)
Times are git committer times, converted to HKT.
- **Theme Radar snapshots and features** (SRoyaltyy/theme-radar, rules 1–12). In-sample uses T = 08-06 (since_first base) through 09-10.
  - `data/snapshots/2026-08-06.csv`: added 6650943, 08-06 18:44 HKT. Last change 94cb047, 08-07 08:57 HKT.
  - `data/snapshots/2026-09-10.csv` (latest T needed): 3897260, 09-11 06:44 HKT. Only one commit.
  - `data/features/2026-09-10_1d.csv`: 924af14, 09-11 06:53 HKT. Only one commit.
  - Every snapshot and feature file in between was also committed before the cutoff. No later edits.
- **fullscan earnings membership** (rules 1–6; read only, not edited). `data/universe/<T+1>_membership.csv` for all 16 in-sample mornings, 08-13..09-11.
  - The last change to any of them was `2026-09-11_membership.csv` @fab16004, 09-11 19:23 HKT.
  - All 16 files match, byte for byte, the copies used in the in-sample run.
- **excel_bot letter panel** (rules 7–12). `excel_bot/research/excel_clear_letter_panel.csv` covers 08-13..09-11.
  - Committed once, in 09fef104 at 09-12 12:32 HKT, and merged to main in 2a1abdb at 09-12 12:33 HKT.
  - The file is unchanged at main@5f0e52b. Its sha256 is 11ff2b1f…9dcc, which matches the in-sample file and the frozen hash.
  - The builder `excel_bot/engine/excel_clear_letter_panel.py` has been at 09fef104 since then.
- **None of the 12 rules needs the post-09-11 letters file** (`letters_oos_0914_0924.csv`, built locally on 09-25). It was used only to score the OOS test I have already seen, not to define any rule.
- **Note:** forward-return labels for T = 09-09 and 09-10 were updated in theme-radar after the cutoff (09-15 07:37 HKT and 09-16 04:49 HKT). These are outcomes, not gate inputs. No rule needs them to fire.

## Eligibility
- **12 of 12 eligible. 0 ineligible.**
- Rule 4 is the one described as "FPE × ER" that survived borrow. It is actually Forward P/E rising vs T-3 combined with the fullscan earnings-this-week gate, not the Excel ER letter.
- Caveat: on the OOS days I have seen, 9 of 12 are TOO THIN or FAIL at 10bp, and 11 of 12 do not survive 15bp + borrow. Those days do not count, per the disclosure above.
