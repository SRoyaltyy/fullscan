# Remaining A–JL families — unmined sweep

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge._

## Plain English

We rebuilt a **real hundreds-of-names** A–JL dump from the Yahoo row cache (never Excel's STOCKHISTORY cache) and scored the leftover cells the first 499-name cut skipped: leftover open numbers, leftover close numbers, leftover green/red highlights, and 0/1 formula flags. Fees are Futubull. We only buy at the open when the sheet already knows the number or the color at 9:30; everything else waits for the close.

A keeper has to work on both ticker halves, both calendar halves (cut 2026-05-01), **Q3** (cut 2026-07-01), both SPY tapes, and must beat 'just buy everyone' by 20 bps. The fattest single day cannot be more than 25% of winning-day P&L. hold1 needs a hold2 sibling; hold5 needs hold2 edge.

Sample **400** tickers (250 discovery / 150 holdout) · lean capture **1.21 s/ticker** · specs **2207** · scored cells **2887**.

**KEEP 0 · KILL 2853 · THIN 34** (raw PASS 0 / FAIL 2853 / THIN 34).

Standing research keeps stay the three light+green O recipes. Finviz volume stays BLOCKED. AB / weather / book stay dead.

### Family scoreboard

| family | what it is | KEEP | KILL | THIN |
|---|---|---:|---:|---:|
| `val_open` | open-knowable numbers past A/C/J | 0 | 330 | 7 |
| `val_close` | close-knowable leftover numbers | 0 | 2082 | 27 |
| `fill_new` | leftover green/red highlights | 0 | 297 | 0 |
| `flag` | 0/1 formula flags | 0 | 144 | 0 |

### Unconditional baseline (this sample, Futubull)

| clock | side | hold | n | avg net | t | win |
|---|---|---:|---:|---:|---:|---:|
| open | long | 1 | 55828 | -0.13% | -6.0 | 45% |
| open | long | 2 | 55428 | +0.90% | 1.6 | 46% |
| open | long | 5 | 54228 | +1.62% | 2.6 | 47% |
| open | short | 1 | 55828 | -0.22% | -10.0 | 46% |
| open | short | 2 | 55428 | -1.25% | -2.2 | 48% |
| open | short | 5 | 54228 | -1.97% | -3.1 | 49% |
| close | long | 1 | 55428 | +1.02% | 1.8 | 45% |
| close | long | 2 | 55028 | +1.28% | 2.1 | 46% |
| close | long | 5 | 53828 | +1.83% | 3.2 | 47% |
| close | short | 1 | 55428 | -1.37% | -2.4 | 46% |
| close | short | 2 | 55028 | -1.63% | -2.7 | 48% |
| close | short | 5 | 53828 | -2.18% | -3.8 | 49% |

### KEEP (hardened)

*(none — every leftover family is a clean null on this sample)*

### Near-miss / top KILL (FAIL, hold1/2 first)

| keep | def | family | clock | side | exit | disc | hold | Q3 | tickers | why |
|---|---|---|---|---|---|---|---|---|---:|---|
| KILL | `valclose_AO_le-2` | val_close | close | short | hold1 | 199/+1.34%/t=4.5 | 124/+1.77%/t=6.0 | — | 323 | thin_disc,date_bar,lottery_day,tape_thin,spy_thin,q3_missing |
| KILL | `valclose_AO_le-1` | val_close | close | short | hold1 | 201/+1.35%/t=4.6 | 126/+1.72%/t=5.9 | — | 327 | thin_disc,date_bar,lottery_day,tape_thin,spy_thin,q3_missing |
| KILL | `valclose_BE_ge2` | val_close | close | long | hold1 | 218/+1.57%/t=6.2 | 127/+1.76%/t=4.8 | — | 345 | thin_disc,date_bar,lottery_day,tape_thin,spy_thin,q3_missing |
| KILL | `valclose_BE_ge1` | val_close | close | long | hold1 | 238/+1.53%/t=5.7 | 139/+1.64%/t=4.7 | — | 377 | thin_disc,date_bar,lottery_day,tape_thin,spy_thin,q3_missing |
| KILL | `valclose_BE_gt0` | val_close | close | long | hold1 | 250/+1.38%/t=4.9 | 150/+1.58%/t=4.5 | — | 400 | thin_disc,date_bar,lottery_day,tape_thin,spy_thin,q3_missing |
| KILL | `valclose_BO_eq1` | val_close | close | long | hold1 | 248/+1.41%/t=4.5 | 149/+1.42%/t=3.7 | 397/+1.41%/t=5.8 | 397 | thin_disc,date_bar,lottery_day,tape_thin,spy_thin |
| KILL | `valclose_BO_ge1` | val_close | close | long | hold1 | 248/+1.41%/t=4.5 | 149/+1.42%/t=3.7 | 397/+1.41%/t=5.8 | 397 | thin_disc,date_bar,lottery_day,tape_thin,spy_thin |
| KILL | `valclose_BO_gt0` | val_close | close | long | hold1 | 249/+1.42%/t=4.5 | 149/+1.42%/t=3.7 | 398/+1.42%/t=5.8 | 397 | thin_disc,date_bar,lottery_day,tape_thin,spy_thin |
| KILL | `valopen_Q_ge1` | val_open | open | long | hold1 | 599/+0.35%/t=1.8 | 356/+1.03%/t=2.7 | 396/+0.80%/t=2.8 | 400 | disc_t,lottery_day |
| KILL | `valclose_BE_ge2` | val_close | close | long | hold2 | 218/+1.30%/t=3.4 | 127/+1.33%/t=2.3 | — | 345 | thin_disc,date_bar,lottery_day,tape_thin,spy_thin,q3_missing,no_edge_vs_uncond |
| KILL | `valclose_BE_gt0` | val_close | close | long | hold2 | 250/+0.89%/t=2.2 | 150/+1.33%/t=2.3 | — | 400 | thin_disc,disc_t,date_bar,lottery_day,tape_thin,spy_thin,q3_missing,no_edge_vs_uncond |
| KILL | `valopen_Q_eq1` | val_open | open | long | hold1 | 328/+0.47%/t=1.8 | 196/+1.35%/t=2.2 | 396/+0.80%/t=2.8 | 398 | disc_t,date_bar,lottery_day |
| KILL | `flag_Q_eq1` | flag | open | long | hold1 | 328/+0.47%/t=1.8 | 196/+1.35%/t=2.2 | 396/+0.80%/t=2.8 | 398 | disc_t,date_bar,lottery_day |
| KILL | `valclose_GV_eq1` | val_close | close | long | hold2 | 8376/+1.09%/t=1.7 | 5133/+13.03%/t=2.0 | 5107/+14.47%/t=2.2 | 400 | disc_t,lottery_day,no_edge_vs_uncond |
| KILL | `valclose_GV_ge1` | val_close | close | long | hold2 | 8376/+1.09%/t=1.7 | 5133/+13.03%/t=2.0 | 5107/+14.47%/t=2.2 | 400 | disc_t,lottery_day,no_edge_vs_uncond |
| KILL | `valclose_GV_gt0` | val_close | close | long | hold2 | 8376/+1.09%/t=1.7 | 5133/+13.03%/t=2.0 | 5107/+14.47%/t=2.2 | 400 | disc_t,lottery_day,no_edge_vs_uncond |
| KILL | `valclose_DJ_eq1` | val_close | close | long | hold2 | 25940/+0.42%/t=2.0 | 15954/+4.15%/t=2.0 | 15943/+4.48%/t=2.1 | 399 | disc_t,no_edge_vs_uncond |
| KILL | `valclose_DJ_ge1` | val_close | close | long | hold2 | 25940/+0.42%/t=2.0 | 15954/+4.15%/t=2.0 | 15943/+4.48%/t=2.1 | 399 | disc_t,no_edge_vs_uncond |
| KILL | `valclose_DJ_gt0` | val_close | close | long | hold2 | 25940/+0.42%/t=2.0 | 15954/+4.15%/t=2.0 | 15943/+4.48%/t=2.1 | 399 | disc_t,no_edge_vs_uncond |
| KILL | `valclose_CZ_ge2` | val_close | close | long | hold2 | 22666/+0.50%/t=2.1 | 13402/+4.93%/t=2.0 | 13682/+5.21%/t=2.1 | 399 | disc_t,hold_t,no_edge_vs_uncond |
| KILL | `valclose_DV_ge1` | val_close | close | long | hold2 | 28937/+0.34%/t=1.8 | 17409/+3.79%/t=2.0 | 17884/+3.90%/t=2.1 | 400 | disc_t,hold_t,no_edge_vs_uncond |
| KILL | `valclose_DV_gt0` | val_close | close | long | hold2 | 28937/+0.34%/t=1.8 | 17409/+3.79%/t=2.0 | 17884/+3.90%/t=2.1 | 400 | disc_t,hold_t,no_edge_vs_uncond |
| KILL | `valclose_DP_ge1` | val_close | close | long | hold2 | 29712/+0.31%/t=1.7 | 17878/+3.69%/t=2.0 | 17889/+3.90%/t=2.1 | 399 | disc_t,hold_t,no_edge_vs_uncond |
| KILL | `valclose_DP_ge2` | val_close | close | long | hold2 | 29712/+0.31%/t=1.7 | 17878/+3.69%/t=2.0 | 17889/+3.90%/t=2.1 | 399 | disc_t,hold_t,no_edge_vs_uncond |
| KILL | `valclose_DP_gt0` | val_close | close | long | hold2 | 29712/+0.31%/t=1.7 | 17878/+3.69%/t=2.0 | 17889/+3.90%/t=2.1 | 399 | disc_t,hold_t,no_edge_vs_uncond |
| KILL | `valclose_S_ge1` | val_close | close | long | hold2 | 29182/+0.33%/t=1.8 | 17560/+3.75%/t=2.0 | 17884/+3.90%/t=2.1 | 400 | disc_t,hold_t,no_edge_vs_uncond |
| KILL | `valclose_S_gt0` | val_close | close | long | hold2 | 29182/+0.33%/t=1.8 | 17560/+3.75%/t=2.0 | 17884/+3.90%/t=2.1 | 400 | disc_t,hold_t,no_edge_vs_uncond |
| KILL | `valclose_FC_ge1` | val_close | close | long | hold2 | 20210/+0.48%/t=1.8 | 12549/+5.25%/t=2.0 | 14437/+4.86%/t=2.1 | 329 | disc_t,hold_t,no_edge_vs_uncond |
| KILL | `valclose_FC_gt0` | val_close | close | long | hold2 | 20210/+0.48%/t=1.8 | 12549/+5.25%/t=2.0 | 14437/+4.86%/t=2.1 | 329 | disc_t,hold_t,no_edge_vs_uncond |
| KILL | `valclose_CZ_ge1` | val_close | close | long | hold2 | 24754/+0.43%/t=2.0 | 14702/+4.47%/t=2.0 | 15078/+4.72%/t=2.1 | 399 | disc_t,hold_t,no_edge_vs_uncond |
| KILL | `valclose_CZ_gt0` | val_close | close | long | hold2 | 24754/+0.43%/t=2.0 | 14702/+4.47%/t=2.0 | 15078/+4.72%/t=2.1 | 399 | disc_t,hold_t,no_edge_vs_uncond |
| KILL | `valclose_CL_gt0` | val_close | close | long | hold2 | 29533/+0.30%/t=1.7 | 17784/+3.70%/t=2.0 | 17956/+3.89%/t=2.1 | 400 | disc_t,hold_t,no_edge_vs_uncond |
| KILL | `valclose_DY_gt0` | val_close | close | long | hold2 | 29883/+0.31%/t=1.7 | 17945/+3.66%/t=2.0 | 17993/+3.87%/t=2.1 | 400 | disc_t,hold_t,no_edge_vs_uncond |
| KILL | `valclose_HP_eq1` | val_close | close | long | hold2 | 18877/+0.41%/t=1.5 | 11324/+5.80%/t=2.0 | 10038/+7.30%/t=2.2 | 400 | disc_t,hold_t,tape_split,no_edge_vs_uncond |
| KILL | `valclose_HP_ge1` | val_close | close | long | hold2 | 18877/+0.41%/t=1.5 | 11324/+5.80%/t=2.0 | 10038/+7.30%/t=2.2 | 400 | disc_t,hold_t,tape_split,no_edge_vs_uncond |
| KILL | `valclose_HP_gt0` | val_close | close | long | hold2 | 18877/+0.41%/t=1.5 | 11324/+5.80%/t=2.0 | 10038/+7.30%/t=2.2 | 400 | disc_t,hold_t,tape_split,no_edge_vs_uncond |

### Ghost that looked like a KEEP (then died)

Column **DE** paints red when the cell equals 0 (green when it equals 1). The formula that writes DE also reads same-day volume and same-day return H, so we only enter at the **close**. `fill_DE_red` short hold5 printed +1.12% / +0.67% on the two ticker halves, but the highlight **never shows up in Q3** (q3 n=0) and the late 2026 half is only 39 trades. hold2 on the same cell is a wash (+0.02% holdout, t=0.08). That is a first-half ghost, not a keeper — **KILL** `q3_missing` / `tape_thin` / `no_hold2_keep`.

### Exhaustion

Every leftover family on this sample is a **clean null**. That is a finding, not a pause: these letters do not buy a leak-free edge under the ship bar in Jan–Sep 2026.

- **open-knowable numbers past A/C/J** (`val_open`): clean null — KEEP 0 / KILL 330 / THIN 7.
- **close-knowable leftover numbers** (`val_close`): clean null — KEEP 0 / KILL 2082 / THIN 27.
- **leftover green/red highlights** (`fill_new`): clean null — KEEP 0 / KILL 297 / THIN 0.
- **0/1 formula flags** (`flag`): clean null — KEEP 0 / KILL 144 / THIN 0.

A–F seed: Yahoo/rows cache via `seed_anchor`. Capture path `lean_rows_cache` · excel STOCKHISTORY cache used: False.

Research only. No cards. Live `flatten_robust` untouched.

