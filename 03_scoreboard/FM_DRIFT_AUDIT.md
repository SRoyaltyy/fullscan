# Factor Mine buy/sell drift audit

Read-only check of the FAIL reported on PR #503. No scoreboard, ledger, or lock row was rewritten. Tree: `origin/main` `ec2cf7cedece881da1ad05f1bef9f20112b50aa4`. The Factor Mine markdown on that tree matches the PR base, so this is the same population the certificate counted.

## Numbers

**5,362 of 13,043 sleeve-days fail. The count is real. All 5,362 predate #335. None are after the past-day lock.**

| | |
|---|---|
| Prime sleeve-days (first commit that printed both 09:30 equity and close equity) | 13,043 |
| Buy/sell fingerprint differs, day still present | 5,201 |
| Day removed after that prime print | 161 |
| Certificate fails (5,201 + 161) | **5,362** |
| Days that exist only as later additions, scored as changes | 0 |
| Renamed or deleted sleeve files | 0 |
| Failures whose text already differed before #335 | **5,362** |
| Buy/sell line changes in #336, or any commit after it | **0** |
| Buy/sell line changes in or after the lock commit | **0** |
| h1 sealed `kind=plan` days that changed | **0 of 7** |

Fingerprint: buy and sell lines only (date, side, ticker, shares). Cover counts as buy, short as sell. Prices, fees, and equity marks are not part of the comparison. History is the full `origin/main` graph (not shallow): 40 commits touch `03_scoreboard/factor_mine`.

## 1. Real drift, with two clumps that are not the artifacts in the question

The four artifact hypotheses do not explain the 5,362.

- **Sleeve renames.** No markdown file under `03_scoreboard/factor_mine` was renamed or deleted in those 40 commits. A drifted day is the same sleeve name at the prime commit and at HEAD.
- **Formatting.** A looser fill parser (optional bold, optional backticks) disagreed with the certificate regex on **0** drifted days. Share strings like `98` and `98.0` normalize to the same count.
- **Share-split adjustment.** Of the 1,166 days that kept the same tickers and sides and changed only the share counts, **0** show a split: the share ratio is not the inverse of the fill-price ratio. Three days are a uniform 2× and two are a uniform 3×, and those prices do not match a split either. The common case is a small whole-share resize after a price reprint (289 days move every name by at most 1 share; 864 stay within 2 shares or 5%). 302 days move at least one name by more than that.
- **New days counted as changes.** **0** sleeve-days have both equities on HEAD and were never in a prime print. Later sessions were appended. They are not in the 5,362.

What the 5,362 actually are:

| Class | Days | What changed |
|---|---:|---|
| Ticker or side | 3,204 | The set of names changed. Median overlap of the (side, ticker) set is 0.45. |
| Shares only | 1,166 | Same names and sides, different share counts. Not a book-wide split. |
| Empty prime | 568 | The first both-equity print had no buy/sell lines. A later print added some. The loose parser and the old `BUY TICKER xN` narrative were also empty on that prime print, so this is not a missed format. |
| Empty now | 263 | The prime print had buy/sell lines. The current print of that day has none. |
| Day deleted | 161 | All 161 are **2026-09-07** (Labor Day, market closed). Printed in `e5d1b89466`, deleted in #172, absent ever since. |

3,489 of the 5,201 still-present diffs were rewritten more than once. The text on `origin/main` for every one of them is still the text from the last scoreboard commit before #335 (`b5946b1d71`, 2026-09-25 13:41 UTC). Nothing after that commit changed a buy/sell fingerprint.

### Examples

**Different names.** `coil_h3_exit_alarm` 2026-08-25.

- Prime print: `5c48e2630cdde7c23c0b2c7905b6fb41f2a7eb86` (2026-09-05, "Factor-mine 09:30 open audit trail and sell-only equity labels (#130)"). Buys: ALIT 88, BMEA 811, CRMD 158, JANX 70, KURA 98, ZURA 206.
- Left that print in: `f5b46d20eec2a7acf5df3924fc5201023d0dcd40` (2026-09-09, "Calibrate factor-mine OPEN/CLOSE marks to official 09:30 and 16:00 prints (#172)"). That commit is still the text on `origin/main`. Buys now: ADIG 59, CRMD 156, KURA 95, LIFE 35, OCUL 118, RZLT 263.

**Same names, shares moved, not a split.** `flatten_h1` 2026-08-25. Same two SHAs. CRMD 223→221, MOS 76→77, OCUL 169→168, RZLT 353→375. #172 reprinted the opens; whole-share sizing moved with the price. The four names did not.

**`union_hot_n4_holdup` 2026-09-21.**

- Prime print: `087d5fda408d2aa419fe21b64034edd0b9e1d356` (2026-09-21, "chore: factor strategy mine 2026-08-13"). SELL INDP 1163, SDGR 141, BRR 1. BUY TJGC 164, LVWR 1682, SECZ 236.
- Left that print in: `41bcc47fe1690a49679b5ee827364c9183eb4d44` (2026-09-23, "chore: factor strategy mine 2026-08-13"). Still the text on `origin/main`. The BRR sale is gone. FEAM 842 was bought instead. TJGC 164→123, LVWR 1682→1261, SECZ 236→176. INDP 1163 and SDGR 141 did not change.

The empty-prime clump is the same kind of revision, not a parser hole. On `coil_h3_exit_alarm` 2026-08-26, #130 has both equities and an OPEN line only: the names are marked `no 09:30 open` and there is no BUY or SELL. #172 then prints SELL BTBT 2, ORBS 4, QTRX 1 and BUY ABX 2 (plus the other names that filled once an official open existed).

## 2. Where the changes sit

### (a) Commit that first moved the day off its prime print

Of the 5,201 days that still exist, the first commit whose buy/sell lines differed from the prime print:

| Days | Commit | When | Title |
|---:|---|---|---|
| 1,288 | `6e140da069` | 2026-09-19 | chore: factor strategy mine 2026-08-13 |
| 1,089 | `f5b46d20ee` | 2026-09-09 | Calibrate factor-mine OPEN/CLOSE marks to official 09:30 and 16:00 prints (#172) |
| 740 | `087d5fda40` | 2026-09-21 | chore: factor strategy mine 2026-08-13 |
| 597 | `75ff1cd1b5` | 2026-09-19 | chore: factor strategy mine 2026-08-13 |
| 357 | `0eab52983` | 2026-09-24 | chore: factor strategy mine 2026-08-13 |
| 242 | `0d3fdf1110` | 2026-09-23 | chore: factor strategy mine 2026-08-13 |
| 190 | `284ca0e3a1` | 2026-09-15 | chore: factor strategy mine 2026-08-13 |
| 165 | `0d87943fa5` | 2026-09-22 | chore: factor strategy mine 2026-08-13 |
| 154 | `c6d666a202` | 2026-09-12 | chore: factor strategy mine 2026-08-13 |
| 112 | `e90be3f49b` | 2026-09-09 | Land 2026-09-08 outputs on the dashboard family (#162) |
| 107 | `41bcc47fe1` | 2026-09-23 | chore: factor strategy mine 2026-08-13 |
| 57 | `cb7df662fa` | 2026-09-19 | chore: factor strategy mine 2026-08-13 |

Those 12 commits are 5,098 of the 5,201. The other 103 are smaller chores in the same 8–24 Sep window. The 161 deleted 2026-09-07 rows were removed by #172 (`f5b46d20ee`), so that commit accounts for 1,089 first-moves plus 161 deletions.

The commit that last wrote the text now on `origin/main` is also entirely before #335. Largest: `087d5fda40` (2,263 days), `0eab52983` (1,166), `6e140da069` (727), #172 (527). The last of those is `0eab52983` on 2026-09-24 21:44 UTC.

### (b) Before vs after #335 / #336

**Yes. Most drift predates #335. All of it does.**

- #335 is `3162e0efd70d5bf1d464f10a95de4fc6f7f8b711` (2026-09-25 21:50 +0800, "Freeze Factor Mine inputs before the 2026-09-25 land"). It does not touch `03_scoreboard/factor_mine`.
- The last scoreboard commit before it is `b5946b1d71` (2026-09-25 13:41 UTC). At that commit, all 5,201 diffs already matched today's fingerprints, and 2026-09-07 was already gone.
- #336 is `5f13a4415ea0cfe460155afb3b7677070a2cd188` (2026-09-25 23:49 +0800, "Weekend Factor Mine rebuild: ledgers, lineups, and point-in-time history"). It adds two order CSVs (`hot4_webull_orders_2026-09-22_2026-09-25.csv`, `union_hot_n4_h1_orders_2026-09-22_2026-09-25.csv`) and changes **0** sleeve markdown buy/sell lines.
- Every later scoreboard publish (`77db2793d6` through `e22a65783b`, nine commits, each rewriting the 339 markdown files) changed **0** existing buy/sell fingerprints. They appended newer sessions and refreshed marks.

| Window | Sleeve-days whose buy/sell lines differ from the prime print |
|---|---:|
| Before #335 | 5,362 |
| In #336 | 0 |
| After #336 | 0 |

The Yahoo rebuild did not produce this FAIL. The daily republish from 2026-08-13, through 24 Sep, did.

### (c) Before vs after the past-day lock

The first `factor_mine` row in `data/past_day_lock/manifest.jsonl` lands in `e22a65783b4edf447289a50f8af345b4e33bb351` (2026-10-06 21:01:38 +0000, "chore: factor strategy mine 2026-08-13"). The seed watermark is 2026-10-05. The file then seals **339** sleeve rows, all dated **2026-10-06**. Days on or before the watermark were not hashed. That is why August and September could already be drifted and still pass the lock: the lock does not cover them.

**Changes after the lock: none.**

- `e22a65783b` itself changed 0 existing buy/sell fingerprints.
- No commit after `e22a65783b` touches `03_scoreboard/factor_mine`.
- All 339 locked 2026-10-06 day-line hashes still match the files on `origin/main` (0 mismatches).

There is no sleeve-day to list.

## 3. Recipes

The drift is spread across the book. Worst sleeves, drifted days out of prime days:

| Drifted | Checked | Sleeve |
|---:|---:|---|
| 28 | 38 | `combo_sh_macd_5050_shared` |
| 25 | 39 | `union_w_hot_cond_h1` |
| 25 | 38 | `combo_seh_333_shared`, `combo_seh_333_skip`, `combo_seh_333_weather`, `combo_seh_403525_shared`, `combo_seh_404020_shared`, `combo_seh_451540_shared`, `combo_seh_502525_shared`, `combo_sh_3070_shared`, `combo_sh_5050_shared`, `combo_sh_7030_shared` |
| 24 | 39 | `union_ret_5_h1`, `union_ret_5_h3` |
| 24 | 38 | `combo_e1s_7030_shared` |

**`union_hot_n4_holdup` is not one of the heavy sleeves.** 5 of 38 prime days differ: 2026-09-17, 2026-09-18, 2026-09-21, 2026-09-22, 2026-09-23.

| Date | What moved | Where |
|---|---|---|
| 2026-09-17 | The only line, BUY BRR 1, is gone. The day is still on the page with no buy/sell. | Prime `087d5fda40` → `41bcc47fe1` |
| 2026-09-18 | Same four names. BUY CYPH 1099→1100. TEM 40, SELL HLP 1829, and SELL SSL 223 unchanged. | same pair of commits |
| 2026-09-21 | See the example above (BRR out, FEAM in, three buy sizes cut). | same pair |
| 2026-09-22 | Prime buys were GRAL 21, NUAI 321, INDP 748 (sells CYPH 1100, TEM 40) at `41bcc47fe1`. Current text, set by `0eab52983` and still on HEAD: sells unchanged, buys CRML 254 and NUAI 321. GRAL and INDP are gone. | `41bcc47fe1` → `0eab52983` |
| 2026-09-23 | Sells unchanged (CRML 254, LVWR 1261, NUAI 321, SECZ 176, TJGC 123). Buys were GLND 1294, INDP 879, XHLD 306. Now GLND 1571, SVIA 943, VKTX 101. | Prime `0d3fdf1110` → `0eab52983` |

## 4. h1

`research/hot_n4_clean_v4/forward_h1/h1_log.jsonl`, `kind=plan` only. Compared to the first commit that contains that day's plan: pick tickers (plan picks have no share count), and planned-sell tickers with share counts. One plan line per day. `dashboard/h1/log.json` matches those seven plans.

| Day | First sealed plan | Buy/sell vs that plan |
|---|---|---|
| 2026-09-28 | `f9c8628a0a5351cde31ee7818615e3544bb02e91` | unchanged |
| 2026-09-29 | `a191348dcde85a1588f907a0f2977411155221ce` | unchanged |
| 2026-09-30 | `da54392a49e1d5614aeedaf064991d1d10e693bd` | unchanged |
| 2026-10-01 | `da54392a49e1d5614aeedaf064991d1d10e693bd` | unchanged |
| 2026-10-02 | `f2996b6c4f9be8629a8eec7fd0c26bbb22ce2bdc` | unchanged |
| 2026-10-05 | `028b4540896b7c3d4a5f2b311107aded8d7f1703` | unchanged |
| 2026-10-06 | `dcec598c3ddf4b65f9e795735b120147431f20f5` | unchanged |

`076e81b315a67473d8c77c2a03d2d7917e1bc6d8` ("Correct the 2026-09-28 open fill") is after the 2026-09-28 plan seal. It does not change that plan's names or sell sizes. h1 is a pass.
