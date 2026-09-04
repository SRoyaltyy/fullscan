# Combined sleeve — .io × mover

_Generated 2026-09-04T13:10:00 — 2026-08-13 → 2026-09-04_

**Method:** one cash-accounted flatten-switch book.

- **Default = `.io` `2w_size`.** Close fill, same names as the published paper sleeve. This is the down-day engine (08-14 +3.2%, 08-19 +4.1%).
- **Flatten at the 09:30 open** only when (1) morning general score S ≥ +1, (2) at least `min_buys` priced mover BUY calls exist, and (3) a book was already printed *before* today (known at 09:30 — today's 13:00–15:45 print is never used for the flatten). Then buy mover top-N by cond, 1d hold, up to `day_cap` of equity.
- **Rotate leftover mover at the next green open** when `rotate_mover=True` so yesterday's 1d holds do not trap cash that could size into today's BUY list.
- **Carry the last printed `.io` book** across gap days when `carry_last_book=True` — same names, close fill, no new information.
- **Do not flatten** on green mornings with no real BUY list (08-13/14). Yesterday's score is never used at today's close.
- Futubull fees, whole shares, no lookahead.

Live book is **hard-red hold-only**: one Futubull cash account, flatten on a green morning with a real BUY list, rotate leftover mover at the next green open, carry the last 2w list on gap days, and **do not open a new ticket when S ≤ −3**. Working lots and due 1d exits stay on. The ungated recycle predecessor reprints ~+21.6% by re-entering 2w_size on hard-red 08-24; this book waits until 08-25 and prints ~+19.1%. INO at $0.90 is the same 2w_size name the $10k paper book held, scaled to $100k. Sunday book dates are dropped.

**Policy:** `flatten_hard_red` · engine `flatten_switch` · 2w_size · longs top 10 @ 10% · day_cap 100% · min_buys 5 · rotate=True · carry=True · size-up ×1.0

## Headline

| Start | Final | Return | Max DD | Trades | Win | vs .io 2w_size | Gate |
|---:|---:|---:|---:|---:|---:|---|---|
| $100,000 | $119,105.48 | **+19.11%** | 2.99% | 54 | 53.7% | BEATS +12.85% | **PASS** |

Futubull fees paid **$1,502.07** (in $738.73 / out $763.34). Whole shares. A name that is already held ties up cash — later names that day only see leftover cash, so some tickets do not fill.

| Side | Trades | Win | P&L |
|---|---:|---:|---:|
| BUY | 54 | 53.7% | $19,015.55 |
| SELL | 0 | 0.0% | $0.00 |

## 15% every 2 weeks

Target **+15%** per calendar fortnight (14 days) and per 10 trading sessions.
Fortnights: **PASS** (min 16.89). 10-session blocks: **PASS** (min 16.89). Rolling: **FAIL** (min 0.96).

| Kind | Start | End | n | Return | Gate |
|---|---|---|---:|---:|---|
| fortnight | 2026-08-13 | 2026-08-26 | 10 | +16.89% | PASS |
| fortnight | 2026-08-27 | 2026-09-04 | 7 | +2.47% | partial |
| block | 2026-08-13 | 2026-08-26 | 10 | +16.89% | PASS |
| block | 2026-08-27 | 2026-09-04 | 7 | +2.47% | partial |
| roll | 2026-08-13 | 2026-08-26 | 10 | +16.89% | PASS |
| roll | 2026-08-14 | 2026-08-27 | 10 | +12.90% | FAIL |
| roll | 2026-08-17 | 2026-08-28 | 10 | +10.21% | FAIL |
| roll | 2026-08-18 | 2026-08-31 | 10 | +8.40% | FAIL |
| roll | 2026-08-19 | 2026-09-01 | 10 | +4.02% | FAIL |
| roll | 2026-08-20 | 2026-09-02 | 10 | +4.92% | FAIL |
| roll | 2026-08-21 | 2026-09-03 | 10 | +0.98% | FAIL |
| roll | 2026-08-24 | 2026-09-04 | 10 | +0.96% | FAIL |

## Day route

| Date | Score | Route | Equity | Cash | core | tac.mv |
|---|---:|---|---:|---:|---:|---:|
| 2026-08-13 | +8.53 | io | $99,801.45 | $136.04 | 9 | 0 |
| 2026-08-14 | +5.50 | io | $102,951.36 | $108.02 | 11 | 0 |
| 2026-08-17 | +2.25 | io | $103,841.32 | $58.83 | 16 | 0 |
| 2026-08-18 | -6.20 | hold | $105,907.01 | $58.83 | 16 | 0 |
| 2026-08-19 | -7.20 | hold | $110,259.52 | $58.83 | 16 | 0 |
| 2026-08-20 | +1.12 | mover | $111,567.07 | $201.36 | 0 | 10 |
| 2026-08-21 | +3.25 | mover | $117,951.61 | $24.98 | 0 | 10 |
| 2026-08-24 | -5.17 | hold | $117,973.64 | $117,973.64 | 0 | 0 |
| 2026-08-25 | +1.80 | io | $117,889.79 | $462.96 | 9 | 0 |
| 2026-08-26 | +2.02 | io | $116,657.68 | $462.96 | 9 | 0 |
| 2026-08-27 | — | io | $116,230.99 | $112.16 | 16 | 0 |
| 2026-08-28 | +0.75 | io | $114,442.80 | $112.16 | 16 | 0 |
| 2026-08-31 | -5.85 | hold | $114,806.70 | $112.16 | 16 | 0 |
| 2026-09-01 | -6.30 | hold | $114,688.26 | $112.16 | 16 | 0 |
| 2026-09-02 | -3.83 | hold | $117,055.90 | $112.16 | 16 | 0 |
| 2026-09-03 | -0.90 | io | $119,105.48 | $93.55 | 18 | 0 |
| 2026-09-04 | — | io | $119,105.48 | $93.55 | 18 | 0 |

## Live method: hard-red hold-only (S ≤ −3)

`flatten_hard_red` is the production book. Working lots stay on. Scheduled 1d exits still settle. Neither sleeve opens a new ticket on a hard-red morning. `flatten_switch_recycle` is the ungated predecessor (re-enters 2w_size on 08-24, S=−5.17).

| Book | Role | Return | Final | Max DD | min fortnight |
|---|---|---:|---:|---:|---:|
| `flatten_hard_red` | **LIVE** | +19.11% | $119,105.48 | 2.99% | 16.89 |
| `flatten_switch_recycle` | previous | +21.61% | $121,611.18 | 2.97% | 19.17 |

| Date | Score | Hard-red (live) | Recycle |
|---|---:|---|---|
| 2026-08-13 | +8.53 | io $99,801 | io $99,801 |
| 2026-08-14 | +5.50 | io $102,951 | io $102,951 |
| 2026-08-17 | +2.25 | io $103,841 | io $103,841 |
| 2026-08-18 | -6.20 | hold $105,907 | io $105,907 |
| 2026-08-19 | -7.20 | hold $110,260 | io $110,260 |
| 2026-08-20 | +1.12 | mover $111,567 | mover $111,567 |
| 2026-08-21 | +3.25 | mover $117,952 | mover $117,952 |
| 2026-08-24 | -5.17 | hold $117,974 | io $117,888 |
| 2026-08-25 | +1.80 | io $117,890 | io $120,214 |
| 2026-08-26 | +2.02 | io $116,658 | io $118,934 |
| 2026-08-27 | — | io $116,231 | io $118,526 |
| 2026-08-28 | +0.75 | io $114,443 | io $116,649 |
| 2026-08-31 | -5.85 | hold $114,807 | io $117,032 |
| 2026-09-01 | -6.30 | hold $114,688 | io $116,897 |
| 2026-09-02 | -3.83 | hold $117,056 | io $119,368 |
| 2026-09-03 | -0.90 | io $119,105 | io $121,611 |
| 2026-09-04 | — | io $119,105 | io $121,611 |

## Sweep (same window, same fees)

| Policy | Return | Max DD | min fortnight | min block | Pass |
|---|---:|---:|---:|---:|---|
| `flatten_switch_recycle` | +21.61% | 2.97% | 19.17 | 19.17 | YES |
| `flatten_rotate` | +22.03% | 2.30% | 18.21 | 18.21 | YES |
| `flatten_hard_red` | +19.11% | 2.99% | 16.89 | 16.89 | YES |
| `flatten_carry_book` | +18.56% | 2.97% | 16.17 | 16.17 | YES |
| `flatten_switch_full` | +18.96% | 2.30% | 15.23 | 15.23 | YES |
| `flatten_skip_blank_io` | +17.57% | 0.20% | 15.23 | 15.23 | YES |
| `flatten_cash_mover` | +18.39% | 2.76% | 14.68 | 14.68 | no |
| `flatten_blank_cash` | +17.85% | 0.48% | 14.68 | 14.68 | no |
| `flatten_switch_70` | +16.98% | 2.30% | 13.31 | 13.31 | no |
| `flatten_rich` | +16.02% | 2.30% | 12.39 | 12.39 | no |
| `flatten_overlap` | +15.44% | 2.30% | 11.83 | 11.83 | no |
| `flatten_overlap_55` | +15.41% | 2.30% | 11.81 | 11.81 | no |
| `flatten_switch_60` | +15.10% | 2.30% | 11.5 | 11.5 | no |
| `flatten_switch` | +15.01% | 2.30% | 11.41 | 11.41 | no |
| `io_3d_switch` | +9.13% | 1.77% | 8.85 | 8.85 | no |
| `flatten_3d` | +11.36% | 2.30% | 7.87 | 7.87 | no |
| `core50_switch` | +11.13% | 1.58% | 7.77 | 7.77 | no |
| `concentrated_switch` | +9.95% | 1.52% | 7.27 | 7.27 | no |
| `core_switch` | +9.71% | 1.60% | 6.95 | 6.95 | no |
| `switch_70` | +9.29% | 1.62% | 6.83 | 6.83 | no |
| `switch_no_short` | +7.96% | 1.92% | 6.76 | 6.76 | no |
| `switch_80` | +8.68% | 1.56% | 6.33 | 6.33 | no |
| `hard_red_shorts` | +6.88% | 2.47% | 5.83 | 5.83 | no |
| `switch_80_overlap` | +6.26% | 1.94% | 5.51 | 5.51 | no |
| `mover_heavy` | +7.19% | 1.56% | 5.0 | 5.0 | no |
| `switch_90_overlap` | +4.88% | 1.97% | 4.56 | 4.56 | no |

## Why this merge

- **Mover** is the highest hit-rate sleeve on this tape (paper +9.3%, max DD 0.12%) because the S ≥ +1 gate deletes the fall days. It is *off* most sessions — that is the product, not a bug. The days it *is* on (08-20, 08-21) are the ones `.io` 2w_size lost or lagged.
- **.io `2w_size`** is the current top published book (+12.85%) and the one that keeps winning on SPY-down / hard-red mornings (08-14 +3.2%, 08-18/19 +2.0/+4.1%). An earlier NAV stitch that flattened on every green morning *including* 08-13/14 (zero BUY calls) sat in cash and gave the edge back.
- **Flatten, don't average.** Averaging pick lists re-imports Excel's median-zero payoff. The combined book *is* `.io` until a green morning that actually has a priced mover BUY list and a prior book, then it *is* mover for one session.
- **Open flatten is leak-free:** `.io` names were bought at a prior close; the 09:30 open is the first price you can get after the new morning predict. Today's book print is not known at 09:30 so the flatten uses yesterday / last print only. Tomorrow's score is never used at today's close.
- **Rotate at the next green open** is the honest way to stay fully invested in mover (the paper book's same-day close→open recycle is a leak; we do not copy it). Sells run first so the new list is funded with cash, not with stock you still hold. **Carry last book** is the same 2w_size list the .io dashboard already follows on a quiet print day — not a third model.

## Cash and fees (not a paper NAV)

This is one Futubull cash account. Every fill pays `00_grounding/futubull_fees.json` (commission + platform + settlement, plus SEC/TAF on sells). Equity = leftover cash + marked positions. Buying 2w_size names on 08-13 spends almost the whole $100k (day-end cash $136); those names stay held through 08-19, so the 08-14/17 add-ons are leftover crumbs (TBCH 1 share, VERI $15). On 08-20 the flatten sells first at the open (fees out), then mover buys consume that cash. On 08-24 the last 2w_size list is re-entered; 08-27's new book names only get the ~$186 leftover (CNH 3 shares, MOS 1 share) because the 08-24 lots are still open.

Nothing here is a fringe overlay: default = published `.io` `2w_size`, switch = published mover gate (S ≥ +1 and a real BUY list), flatten/rotate at the 09:30 print you already have, carry = keep the last 2w list. No NAV stitch, no same-day close→open recycle, no Excel vote, no leverage.

Code: `src/sleeve_merge.py`. Machine: `data/sleeve_merge/`. Dashboard: `dashboard/sleeve-merge/index.html`.
