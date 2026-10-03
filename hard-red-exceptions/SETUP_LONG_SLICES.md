# HRE long setup slices

Longs do **not** have a durable edge in this sample. Base LONG is still a coin-flip: **47.9% / +0.05% H1** (n=4538 holds, 544 missing). Do not dress that up.

Scanned **1247** ticker histories (`dashboard/hard-red-exceptions/t/*.json`) · 2026-08-13 → 2026-09-16 · 29928 session rows · 8268 setups (5082 LONG / 3186 SHORT). Classifier is `open_camera_setup` / `setupOf`: 5 numeric opens including today; LONG = cheapest or 2nd-cheapest **and** camera net = n_pos − n_neg rose vs prior. Clean = n_neg did not rise and n_pos did not fall. Explore = net rose but the other color deteriorated. Win = stored h1/h3/h5 as **long** P&L (not flipped). Win rate counts `pct > 0`. Missing-hold n is separate from setup n. KEEP still wants >55% after fees and n≥30.

## 1. Short answer

**Almost nowhere.** Three printed cells hit ~55% H1 with hold-n≥30. Only one of them is multi-date:

| cell | H1 win / mean (n) | why it is or is not a situation |
|---|---|---|
| **on_list × clean** | **55.3% / +1.01% (n=311, miss=0)** | 17 dates. Ex-2026-09-03 still 56.8% (n=250). H3 52.7%; **H5 dies 39.9% / −1.43%**. Shopping-list names only. After fees, not a wire. |
| clean × n_neg==0 | 55.6% / +1.71% (n=54, miss=3) | Setup n=57. Thin. 9 dates; 18/57 on 2026-08-20. Ex-9/03: 53.3% (n=45). |
| −3<S≤0 | 58.7% / +1.30% (n=341, miss=0) | **One morning: 2026-09-03, S=−0.9.** Not a weather-bucket edge. H3 43.1%, H5 41.4%. |

Everywhere else with n≥30 is **47–54% H1**. Closest large-n runner-up that is not a one-day cluster: **net_delta ≥+4 at 54.3% / +0.81% (n=639)** — drops to 51.7% (n=530) if 2026-09-03 is removed. Tighter long (lowest + clean + net≥+2) is 49.6%. **No long edge in this sample.**

## Traps

| trap | H1 | H3 | H5 | note |
|---|---|---|---|---|
| **explore longs** | 46.9% / −0.09% (n=652, miss=0) | 39.7% / −1.39% (n=642) | 37.8% / −1.88% (n=571) | Net rose the wrong way on the other color. Worse as you hold. |
| **2nd-lowest only** | 47.8% / +0.07% (n=1758, miss=169) | 43.2% / −0.70% (n=1580) | 39.5% / −1.25% (n=1265) | Same coin-flip as lowest; fades harder past H1. |
| **hard-red explore longs** | **35.9% / −1.69% (n=145, miss=0)** | 48.9% / −1.20% (n=135) | 30.7% / −3.03% (n=114) | Worst long cell with n>100. Do not long a SIT morning explore. |
| **−3<S≤0 weather bucket** | 58.7% / +1.30% (n=341) | 43.1% / −0.13% (n=341) | 41.4% / −1.15% (n=341) | One date. S is market-wide; this is a calendar cluster. |
| 3-day window instead of 5 | 46.2% / −0.08% (n=6524, miss=715) | 42.5% / −0.70% (n=5972) | 40.7% / −1.06% (n=4959) | Looser cheap-rank. Worse than base. |

## 2. Open rank

| slice | n setups | H1 win / mean | H3 win / mean | H5 win / mean |
|---|---:|---|---|---|
| lowest | 3155 | 48.0% / +0.04% (n=2780, miss=375) | 46.0% / −0.26% (n=2502, miss=653) | 43.5% / −0.70% (n=2052, miss=1103) |
| 2nd-lowest | 1927 | 47.8% / +0.07% (n=1758, miss=169) | 43.2% / −0.70% (n=1580, miss=347) | 39.5% / −1.25% (n=1265, miss=662) |
| lowest OR 2nd (base LONG) | 5082 | 47.9% / +0.05% (n=4538, miss=544) | 44.9% / −0.43% (n=4082, miss=1000) | 42.0% / −0.91% (n=3317, miss=1765) |

Lowest vs 2nd-lowest is a wash at H1. 2nd-lowest fades more at H3/H5.

## 3. Quality

| slice | n setups | H1 win / mean | H3 win / mean | H5 win / mean |
|---|---:|---|---|---|
| clean | 4430 | 48.1% / +0.08% (n=3886, miss=544) | 45.9% / −0.25% (n=3440, miss=990) | 42.9% / −0.71% (n=2746, miss=1684) |
| explore | 652 | 46.9% / −0.09% (n=652, miss=0) | 39.7% / −1.39% (n=642, miss=10) | 37.8% / −1.88% (n=571, miss=81) |
| all | 5082 | 47.9% / +0.05% (n=4538, miss=544) | 44.9% / −0.43% (n=4082, miss=1000) | 42.0% / −0.91% (n=3317, miss=1765) |

Clean does not rescue H1. Explore is the worse tail.

## 4. net_delta buckets

| slice | n setups | H1 win / mean | H3 win / mean | H5 win / mean |
|---|---:|---|---|---|
| +1 | 1771 | 45.6% / −0.39% (n=1683, miss=88) | 41.5% / −0.99% (n=1483, miss=288) | 39.7% / −1.52% (n=1166, miss=605) |
| +2 | 1556 | 48.3% / +0.28% (n=1376, miss=180) | 46.0% / −0.26% (n=1247, miss=309) | 40.1% / −1.11% (n=1003, miss=553) |
| +3 | 950 | 47.0% / −0.00% (n=840, miss=110) | 47.0% / −0.25% (n=764, miss=186) | 42.9% / −0.70% (n=638, miss=312) |
| ≥+4 | 805 | 54.3% / +0.81% (n=639, miss=166) | 48.5% / +0.38% (n=588, miss=217) | 49.8% / +0.62% (n=510, miss=295) |
| ≥+2 | 3311 | 49.2% / +0.31% (n=2855, miss=456) | 46.9% / −0.11% (n=2599, miss=712) | 43.2% / −0.58% (n=2151, miss=1160) |

≥+4 is the closest large-n H1 cell (54.3%). Strip 2026-09-03 and it is 51.7% (n=530). Not 55%.

## 5. hard_red / SIT

| slice | n setups | H1 win / mean | H3 win / mean | H5 win / mean |
|---|---:|---|---|---|
| hard_red / SIT | 1929 | 46.7% / −0.12% (n=1928, miss=1) | 52.2% / +0.56% (n=1472, miss=457) | 41.6% / −0.68% (n=1190, miss=739) |
| not-sit | 3153 | 48.8% / +0.19% (n=2610, miss=543) | 40.8% / −0.99% (n=2610, miss=543) | 42.2% / −1.04% (n=2127, miss=1026) |
| all | 5082 | 47.9% / +0.05% (n=4538, miss=544) | 44.9% / −0.43% (n=4082, miss=1000) | 42.0% / −0.91% (n=3317, miss=1765) |

SIT longs are not a bounce sleeve at H1. H3 52.2% on hard-red is still below KEEP and fades at H5.

## 6. Prior net / today cameras

| slice | n setups | H1 win / mean | H3 win / mean | H5 win / mean |
|---|---:|---|---|---|
| prior net ≤0 | 4214 | 47.8% / +0.09% (n=3743, miss=471) | 45.3% / −0.29% (n=3348, miss=866) | 41.7% / −0.79% (n=2716, miss=1498) |
| prior net >0 | 868 | 48.4% / −0.11% (n=795, miss=73) | 43.1% / −1.09% (n=734, miss=134) | 43.4% / −1.48% (n=601, miss=267) |
| today n_neg==0 | 119 | 44.8% / −0.42% (n=116, miss=3) | 39.7% / −2.19% (n=116, miss=3) | 36.8% / −2.90% (n=114, miss=5) |
| today n_neg≥1 | 4963 | 48.0% / +0.07% (n=4422, miss=541) | 45.1% / −0.38% (n=3966, miss=997) | 42.2% / −0.84% (n=3203, miss=1760) |
| today n_pos≥3 | 3755 | 49.6% / +0.16% (n=3276, miss=479) | 46.6% / −0.22% (n=2945, miss=810) | 43.4% / −0.69% (n=2466, miss=1289) |
| today n_pos<3 | 1327 | 43.5% / −0.22% (n=1262, miss=65) | 40.6% / −0.97% (n=1137, miss=190) | 38.0% / −1.54% (n=851, miss=476) |

Zero-red today, by itself, is a loser (44.8%). n_pos<3 is worse than n_pos≥3; neither clears 55%.

## 7. Reconstructed vs on_list

| slice | n setups | H1 win / mean | H3 win / mean | H5 win / mean |
|---|---:|---|---|---|
| reconstructed | 4641 | 47.7% / +0.04% (n=4097, miss=544) | 44.5% / −0.43% (n=3653, miss=988) | 42.9% / −0.73% (n=2912, miss=1729) |
| on_list | 441 | 50.1% / +0.22% (n=441, miss=0) | 48.3% / −0.43% (n=429, miss=12) | 35.8% / −2.18% (n=405, miss=36) |
| neither / both | 0 | — | — | — |

Every long is either reconstructed or on_list (0 overlap, 0 leftover). On-list alone is 50.1% H1; the 55.3% print needs the clean cut on top.

## 8. Weather S buckets

S is market-wide (one value per session). This is a calendar cut, not a name cut.

| slice | n setups | dates | H1 win / mean | H3 win / mean | H5 win / mean |
|---|---:|---:|---|---|---|
| S≤−3 | 1929 | sit mornings | 46.7% / −0.12% (n=1928, miss=1) | 52.2% / +0.56% (n=1472, miss=457) | 41.6% / −0.68% (n=1190, miss=739) |
| −3<S≤0 | 341 | **1 (2026-09-03)** | 58.7% / +1.30% (n=341, miss=0) | 43.1% / −0.13% (n=341, miss=0) | 41.4% / −1.15% (n=341, miss=0) |
| S>0 | 2727 | green mornings | 47.8% / +0.10% (n=2184, miss=543) | 40.9% / −1.05% (n=2184, miss=543) | 42.5% / −1.06% (n=1701, miss=1026) |
| S missing | 85 | **1 (2026-08-27)** | 32.9% / −2.17% (n=85, miss=0) | 30.6% / −2.75% (n=85, miss=0) | 40.0% / −0.35% (n=85, miss=0) |

S≤−3 matches hard_red exactly in this file set (same 1929 / same holds). S missing is 2026-08-27 only.

## 9. Calendar half

24 session dates, split 12 / 12.

| slice | n setups | H1 win / mean | H3 win / mean | H5 win / mean |
|---|---:|---|---|---|
| first half 2026-08-13–2026-08-28 | 1712 | 47.3% / +0.10% (n=1712, miss=0) | 43.9% / −0.64% (n=1712, miss=0) | 43.6% / −0.67% (n=1712, miss=0) |
| second half 2026-08-31–2026-09-16 | 3370 | 48.3% / +0.02% (n=2826, miss=544) | 45.7% / −0.28% (n=2370, miss=1000) | 40.3% / −1.17% (n=1605, miss=1765) |

No regime where longs work. Missing H1/H3/H5 are all in the second half (tail of the window).

## 10. Adjacent long rules (each vs base, not stacked)

| rule | n setups | H1 win / mean | H3 win / mean | H5 win / mean |
|---|---:|---|---|---|
| base LONG | 5082 | 47.9% / +0.05% (n=4538, miss=544) | 44.9% / −0.43% (n=4082, miss=1000) | 42.0% / −0.91% (n=3317, miss=1765) |
| lowest only | 3155 | 48.0% / +0.04% (n=2780, miss=375) | 46.0% / −0.26% (n=2502, miss=653) | 43.5% / −0.70% (n=2052, miss=1103) |
| require clean | 4430 | 48.1% / +0.08% (n=3886, miss=544) | 45.9% / −0.25% (n=3440, miss=990) | 42.9% / −0.71% (n=2746, miss=1684) |
| require net_delta ≥ +2 | 3311 | 49.2% / +0.31% (n=2855, miss=456) | 46.9% / −0.11% (n=2599, miss=712) | 43.2% / −0.58% (n=2151, miss=1160) |
| require n_neg did not rise | 4632 | 47.8% / +0.04% (n=4088, miss=544) | 45.5% / −0.34% (n=3642, miss=990) | 42.2% / −0.84% (n=2877, miss=1755) |
| require today n_neg == 0 | 119 | 44.8% / −0.42% (n=116, miss=3) | 39.7% / −2.19% (n=116, miss=3) | 36.8% / −2.90% (n=114, miss=5) |
| tighter: THE low of 5 AND clean AND net≥+2 | 1892 | 49.6% / +0.35% (n=1575, miss=317) | 48.6% / +0.20% (n=1419, miss=473) | 45.6% / −0.24% (n=1168, miss=724) |
| 3-day window (cheapest or 2nd of last 3) | 7239 | 46.2% / −0.08% (n=6524, miss=715) | 42.5% / −0.70% (n=5972, miss=1267) | 40.7% / −1.06% (n=4959, miss=2280) |
| prior session day_pct < 0 | 3875 | 46.4% / −0.14% (n=3387, miss=488) | 43.6% / −0.66% (n=3080, miss=795) | 40.8% / −1.11% (n=2484, miss=1391) |
| fresh (prior day was not also long) | 4044 | 47.3% / −0.02% (n=3660, miss=384) | 45.8% / −0.32% (n=3276, miss=768) | 43.5% / −0.62% (n=2635, miss=1409) |
| not fresh | 1038 | 50.5% / +0.34% (n=878, miss=160) | 41.4% / −0.89% (n=806, miss=232) | 36.4% / −2.04% (n=682, miss=356) |

Prior day_pct is present on every long (miss=0). 3-day is a different classifier (not a subset): 7239 vs 5082 base. Fresh is not better than base. No adjacent cut reaches 55% H1.

3-day extras: lowest-only 46.6% H1 (n=3694); clean 46.1% (n=5557); explore 46.8% (n=967).

## 11. Requested crosses

| cross | n setups | H1 win / mean | H3 win / mean | H5 win / mean |
|---|---:|---|---|---|
| lowest × clean × net≥+2 | 1892 | 49.6% / +0.35% (n=1575, miss=317) | 48.6% / +0.20% (n=1419, miss=473) | 45.6% / −0.24% (n=1168, miss=724) |
| lowest × prior net≤0 | 2655 | 47.9% / +0.06% (n=2314, miss=341) | 46.1% / −0.15% (n=2071, miss=584) | 43.3% / −0.57% (n=1690, miss=965) |
| clean × n_neg==0 | 57 | 55.6% / +1.71% (n=54, miss=3) **thin** | 51.9% / +0.35% (n=54, miss=3) | 51.9% / −0.48% (n=52, miss=5) |
| hard_red × lowest × clean | 1154 | 48.2% / +0.03% (n=1153, miss=1) | 53.7% / +0.86% (n=885, miss=269) | 44.3% / −0.30% (n=690, miss=464) |
| not-sit × lowest × clean × net≥+2 | 1215 | 50.1% / +0.43% (n=898, miss=317) | 43.1% / −0.49% (n=898, miss=317) | 46.4% / −0.32% (n=743, miss=472) |

## 12. Extra two-ways (n≥30 unless marked thin)

| cross | n setups | H1 win / mean | H3 win / mean | H5 win / mean |
|---|---:|---|---|---|
| lowest × clean | 2714 | 48.3% / +0.02% (n=2339, miss=375) | 47.2% / −0.11% (n=2071, miss=643) | 44.6% / −0.56% (n=1660, miss=1054) |
| lowest × explore | 441 | 46.3% / +0.15% (n=441, miss=0) | 40.6% / −0.97% (n=431, miss=10) | 39.0% / −1.32% (n=392, miss=49) |
| 2nd-lowest × clean | 1716 | 47.8% / +0.16% (n=1547, miss=169) | 44.0% / −0.47% (n=1369, miss=347) | 40.2% / −0.94% (n=1086, miss=630) |
| 2nd-lowest × explore | 211 | 48.3% / −0.59% (n=211, miss=0) | 37.9% / −2.24% (n=211, miss=0) | 35.2% / −3.10% (n=179, miss=32) |
| hard_red × explore | 145 | 35.9% / −1.69% (n=145, miss=0) | 48.9% / −1.20% (n=135, miss=10) | 30.7% / −3.03% (n=114, miss=31) |
| hard_red × clean | 1784 | 47.6% / +0.00% (n=1783, miss=1) | 52.5% / +0.73% (n=1337, miss=447) | 42.8% / −0.43% (n=1076, miss=708) |
| hard_red × lowest | 1256 | 47.3% / −0.08% (n=1255, miss=1) | 53.7% / +0.71% (n=977, miss=279) | 43.4% / −0.58% (n=768, miss=488) |
| hard_red × 2nd-lowest | 673 | 45.6% / −0.22% (n=673, miss=0) | 49.1% / +0.24% (n=495, miss=178) | 38.4% / −0.85% (n=422, miss=251) |
| not-sit × explore | 507 | 50.1% / +0.37% (n=507, miss=0) | 37.3% / −1.44% (n=507, miss=0) | 39.6% / −1.59% (n=457, miss=50) |
| not-sit × clean | 2646 | 48.5% / +0.14% (n=2103, miss=543) | 41.7% / −0.88% (n=2103, miss=543) | 42.9% / −0.89% (n=1670, miss=976) |
| on_list × clean | 311 | **55.3% / +1.01% (n=311, miss=0)** | 52.7% / +0.55% (n=300, miss=11) | 39.9% / −1.43% (n=276, miss=35) |
| reconstructed × clean | 4119 | 47.4% / −0.00% (n=3575, miss=544) | 45.3% / −0.33% (n=3140, miss=979) | 43.2% / −0.63% (n=2470, miss=1649) |
| S>0 × lowest × clean | 1380 | 47.4% / −0.09% (n=1006, miss=374) | 42.7% / −1.03% (n=1006, miss=374) | 45.8% / −0.81% (n=790, miss=590) |
| prior net≤0 × clean | 3651 | 48.1% / +0.14% (n=3180, miss=471) | 46.5% / −0.06% (n=2794, miss=857) | 42.7% / −0.54% (n=2220, miss=1431) |
| prior net>0 × clean | 779 | 48.2% / −0.20% (n=706, miss=73) | 43.2% / −1.09% (n=646, miss=133) | 43.7% / −1.44% (n=526, miss=253) |
| n_neg==0 × lowest | 66 | 39.4% / −0.53% (n=66, miss=0) | 34.8% / −3.34% (n=66, miss=0) | 30.3% / −4.07% (n=66, miss=0) |
| n_pos≥3 × clean | 3292 | 49.6% / +0.16% (n=2813, miss=479) | 47.4% / −0.09% (n=2492, miss=800) | 43.8% / −0.56% (n=2043, miss=1249) |
| fresh × clean | 3582 | 47.9% / +0.05% (n=3198, miss=384) | 46.4% / −0.17% (n=2822, miss=760) | 44.4% / −0.45% (n=2232, miss=1350) |
| prior day_pct<0 × clean | 3359 | 46.6% / −0.17% (n=2871, miss=488) | 44.5% / −0.52% (n=2573, miss=786) | 41.6% / −0.97% (n=2029, miss=1330) |
| first-half × clean | 1291 | 46.1% / +0.05% (n=1291, miss=0) | 45.5% / −0.35% (n=1291, miss=0) | 44.5% / −0.46% (n=1291, miss=0) |
| second-half × clean | 3139 | 49.1% / +0.09% (n=2595, miss=544) | 46.1% / −0.19% (n=2149, miss=990) | 41.4% / −0.93% (n=1455, miss=1684) |
| hard_red × lowest × clean × net≥+2 | 677 | 48.9% / +0.24% (n=677, miss=0) | 58.2% / +1.40% (n=521, miss=156) | 44.0% / −0.12% (n=425, miss=252) |
| on_list × lowest × clean | 203 | 54.2% / +0.74% (n=203, miss=0) | 52.6% / +0.44% (n=194, miss=9) | 37.3% / −1.83% (n=177, miss=26) |

hard_red × lowest × clean × net≥+2 H3 58.2% is a hold-3 bounce on sit mornings, not H1, and H5 is 44.0%. Not a long-H1 situation.

## Honesty

- Numbers recomputed from the 1247 checked-in histories with the merged `open_camera_setup` rule. Base LONG H1 **47.9% / +0.05% (n=4538)** matches `SETUP_BACKTEST.md`.
- Weather S is one number per date. −3<S≤0 = 2026-09-03 only. S missing = 2026-08-27 only.
- on_list × clean is the only multi-date ~55% H1 cell with n≥30. H5 39.9%. Shopping-list selection, not a general long rule.
- Explore longs, 2nd-lowest-only, and hard-red explore longs are the cells to stay away from.
- Live sit / flatten_robust / Webull untouched.

Dashboard: [hard-red-exceptions](./index.html). Sibling backtest: [SETUP_BACKTEST.md](./SETUP_BACKTEST.md).
