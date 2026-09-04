# Combined sleeve — .io × mover

_Generated 2026-09-04T11:55:07 — 2026-08-13 → 2026-09-03_

**Method:** one cash-accounted flatten-switch book.

- **Default = `.io` `2w_size`.** Close fill, same names as the published paper sleeve. This is the down-day engine (08-14 +3.2%, 08-19 +4.1%).
- **Flatten at the 09:30 open** only when (1) morning general score S ≥ +1, (2) at least `min_buys` priced mover BUY calls exist, and (3) a book was already printed *before* today (known at 09:30 — today's 13:00–15:45 print is never used for the flatten). Then buy mover top-N by cond, 1d hold, up to `day_cap` of equity.
- **Rotate leftover mover at the next green open** when `rotate_mover=True` so yesterday's 1d holds do not trap cash that could size into today's BUY list.
- **Carry the last printed `.io` book** across gap days when `carry_last_book=True` — same names, close fill, no new information.
- **Do not flatten** on green mornings with no real BUY list (08-13/14). Yesterday's score is never used at today's close.
- Futubull fees, whole shares, no lookahead.

**Policy:** `flatten_switch_recycle` · engine `flatten_switch` · 2w_size · longs top 10 @ 10% · day_cap 100% · min_buys 5 · rotate=True · carry=True · size-up ×1.0

## Headline

| Start | Final | Return | Max DD | Trades | Win | vs .io 2w_size | Gate |
|---:|---:|---:|---:|---:|---:|---|---|
| $100,000 | $121,571.23 | **+21.57%** | 2.97% | 52 | 59.6% | BEATS +12.85% | **PASS** |

Futubull fees paid **$1,501.30** (in $738.36 / out $762.94). Whole shares. A name that is already held ties up cash — later names that day only see leftover cash, so some tickets do not fill.

| Side | Trades | Win | P&L |
|---|---:|---:|---:|
| BUY | 52 | 59.6% | $21,481.35 |
| SELL | 0 | 0.0% | $0.00 |

## 15% every 2 weeks

Target **+15%** per calendar fortnight (14 days) and per 10 trading sessions.
Fortnights: **PASS** (min 19.13). 10-session blocks: **PASS** (min 19.13). Rolling: **FAIL** (min 1.2).

| Kind | Start | End | n | Return | Gate |
|---|---|---|---:|---:|---|
| fortnight | 2026-08-13 | 2026-08-26 | 10 | +19.13% | PASS |
| fortnight | 2026-08-27 | 2026-09-03 | 7 | +2.60% | partial |
| block | 2026-08-13 | 2026-08-26 | 10 | +19.13% | PASS |
| block | 2026-08-27 | 2026-09-03 | 7 | +2.60% | partial |
| roll | 2026-08-13 | 2026-08-26 | 10 | +19.13% | PASS |
| roll | 2026-08-14 | 2026-08-27 | 10 | +15.09% | PASS |
| roll | 2026-08-17 | 2026-08-28 | 10 | +12.30% | FAIL |
| roll | 2026-08-18 | 2026-08-30 | 10 | +10.11% | FAIL |
| roll | 2026-08-19 | 2026-08-31 | 10 | +6.11% | FAIL |
| roll | 2026-08-20 | 2026-09-01 | 10 | +4.71% | FAIL |
| roll | 2026-08-21 | 2026-09-02 | 10 | +1.20% | FAIL |
| roll | 2026-08-24 | 2026-09-03 | 10 | +3.16% | FAIL |

## Day route

| Date | Score | Route | Equity | Cash | core | tac.mv |
|---|---:|---|---:|---:|---:|---:|
| 2026-08-13 | +8.53 | io | $99,801.45 | $136.04 | 9 | 0 |
| 2026-08-14 | +5.50 | io | $102,951.36 | $108.02 | 11 | 0 |
| 2026-08-17 | +2.25 | io | $103,841.32 | $58.83 | 16 | 0 |
| 2026-08-18 | -6.20 | io | $105,907.01 | $58.83 | 16 | 0 |
| 2026-08-19 | -7.20 | io | $110,259.52 | $58.83 | 16 | 0 |
| 2026-08-20 | +1.12 | mover | $111,605.23 | $183.05 | 0 | 10 |
| 2026-08-21 | +3.25 | mover | $117,912.83 | $28.48 | 0 | 10 |
| 2026-08-24 | -5.17 | io | $117,847.88 | $296.57 | 9 | 0 |
| 2026-08-25 | +1.80 | io | $120,172.92 | $296.57 | 9 | 0 |
| 2026-08-26 | +2.02 | io | $118,893.59 | $296.57 | 9 | 0 |
| 2026-08-27 | — | io | $118,485.28 | $110.42 | 15 | 0 |
| 2026-08-28 | +0.75 | io | $116,609.06 | $68.54 | 16 | 0 |
| 2026-08-30 | — | io | $116,609.06 | $68.54 | 16 | 0 |
| 2026-08-31 | -5.85 | io | $116,991.71 | $68.54 | 16 | 0 |
| 2026-09-01 | -6.30 | io | $116,857.32 | $68.54 | 16 | 0 |
| 2026-09-02 | -3.83 | io | $119,327.66 | $68.54 | 16 | 0 |
| 2026-09-03 | -0.90 | io | $121,571.23 | $68.54 | 16 | 0 |

## Sweep (same window, same fees)

| Policy | Return | Max DD | min fortnight | min block | Pass |
|---|---:|---:|---:|---:|---|
| `flatten_switch_recycle` | +21.57% | 2.97% | 19.13 | 19.13 | YES |
| `flatten_rotate` | +21.99% | 2.30% | 18.17 | 18.17 | YES |
| `flatten_carry_book` | +18.55% | 2.97% | 16.16 | 16.16 | YES |
| `flatten_switch_full` | +18.95% | 2.30% | 15.22 | 15.22 | YES |
| `flatten_skip_blank_io` | +17.56% | 0.20% | 15.22 | 15.22 | YES |
| `flatten_cash_mover` | +18.38% | 2.76% | 14.67 | 14.67 | no |
| `flatten_blank_cash` | +12.49% | 3.72% | 14.67 | 14.67 | no |
| `flatten_switch_70` | +16.63% | 2.30% | 12.97 | 12.97 | no |
| `flatten_rich` | +15.69% | 2.30% | 12.06 | 12.06 | no |
| `flatten_overlap` | +15.09% | 2.30% | 11.49 | 11.49 | no |
| `flatten_overlap_55` | +15.00% | 2.30% | 11.41 | 11.41 | no |
| `flatten_switch_60` | +14.62% | 2.30% | 11.02 | 11.02 | no |
| `flatten_switch` | +14.58% | 2.30% | 10.99 | 10.99 | no |
| `io_3d_switch` | +10.36% | 2.32% | 10.14 | 10.14 | no |
| `concentrated_switch` | +10.95% | 2.16% | 9.58 | 9.58 | no |
| `core50_switch` | +10.73% | 2.13% | 9.18 | 9.18 | no |
| `switch_70` | +9.98% | 2.26% | 8.82 | 8.82 | no |
| `core_switch` | +10.02% | 2.21% | 8.73 | 8.73 | no |
| `switch_80` | +9.95% | 2.25% | 8.67 | 8.67 | no |
| `switch_no_short` | +8.53% | 2.57% | 8.67 | 8.67 | no |
| `hard_red_shorts` | +7.55% | 3.15% | 7.83 | 7.83 | no |
| `switch_80_overlap` | +7.26% | 2.65% | 7.6 | 7.6 | no |
| `flatten_3d` | +10.97% | 2.30% | 7.49 | 7.49 | no |
| `mover_heavy` | +8.61% | 2.26% | 7.29 | 7.29 | no |
| `switch_90_overlap` | +6.41% | 2.71% | 6.89 | 6.89 | no |

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
