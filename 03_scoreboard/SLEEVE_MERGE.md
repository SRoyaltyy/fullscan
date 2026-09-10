# Combined sleeve — .io × mover

_Generated 2026-09-10T05:35:16 — 2026-08-13 → 2026-09-09_

**Method:** one cash-accounted flatten-switch book.

- **Default = `.io` `2w_size`.** Close fill, same names as the published paper sleeve. This is the down-day engine (08-14 +3.2%, 08-19 +4.1%).
- **Flatten at the 09:30 open** only when (1) morning general score S ≥ +1, (2) at least `min_buys` priced mover BUY calls exist, and (3) a book was already printed *before* today (known at 09:30 — today's 13:00–15:45 print is never used for the flatten). Then buy mover top-N by cond, 1d hold, up to `day_cap` of equity.
- **Rotate leftover mover at the next green open** when `rotate_mover=True` so yesterday's 1d holds do not trap cash that could size into today's BUY list.
- **Carry the last printed `.io` book** across gap days when `carry_last_book=True` — same names, close fill, no new information.
- **Do not flatten** on green mornings with no real BUY list (08-13/14). Yesterday's score is never used at today's close.
- Futubull fees, whole shares, no lookahead.

Live book is **hard-red hold-only**: one Futubull cash account, flatten on a green morning with a real BUY list, rotate leftover mover at the next green open, carry the last 2w list on gap days, and **do not open a new ticket when S ≤ −3**. Working lots and due 1d exits stay on. The ungated recycle predecessor reprints ~+21.6% by re-entering 2w_size on hard-red 08-24; this book waits until 08-25 and prints ~+19.1%. INO at $0.90 is the same 2w_size name the $10k paper book held, scaled to $100k. Sunday book dates are dropped.

**Policy:** `flatten_robust` · engine `flatten_switch` · 3d_size · longs top 10 @ 10% · day_cap 100% · min_buys 5 · rotate=True · carry=True · size-up ×1.0

## Headline

| Start | Final | Return | Max DD | Trades | Win | vs .io 2w_size | Gate |
|---:|---:|---:|---:|---:|---:|---|---|
| $100,000 | $108,616.66 | **+8.62%** | 7.12% | 72 | 45.8% | trails +10.26% | **FAIL** |

Futubull fees paid **$2,622.22** (in $1,294.73 / out $1,327.49). Whole shares. A name that is already held ties up cash — later names that day only see leftover cash, so some tickets do not fill.

| Side | Trades | Win | P&L |
|---|---:|---:|---:|
| BUY | 72 | 45.8% | $8,616.68 |
| SELL | 0 | 0.0% | $0.00 |

## 15% every 2 weeks

Target **+15%** per calendar fortnight (14 days) and per 10 trading sessions.
Fortnights: **FAIL** (min -2.0). 10-session blocks: **FAIL** (min 12.24). Rolling: **FAIL** (min -3.38).

| Kind | Start | End | n | Return | Gate |
|---|---|---|---:|---:|---|
| fortnight | 2026-08-13 | 2026-08-26 | 10 | +12.24% | FAIL |
| fortnight | 2026-08-27 | 2026-09-09 | 9 | -2.00% | FAIL |
| block | 2026-08-13 | 2026-08-26 | 10 | +12.24% | FAIL |
| block | 2026-08-27 | 2026-09-09 | 9 | -2.00% | partial |
| roll | 2026-08-13 | 2026-08-26 | 10 | +12.24% | FAIL |
| roll | 2026-08-14 | 2026-08-27 | 10 | +8.82% | FAIL |
| roll | 2026-08-17 | 2026-08-28 | 10 | +5.10% | FAIL |
| roll | 2026-08-18 | 2026-08-31 | 10 | +6.96% | FAIL |
| roll | 2026-08-19 | 2026-09-01 | 10 | +9.33% | FAIL |
| roll | 2026-08-20 | 2026-09-02 | 10 | +10.87% | FAIL |
| roll | 2026-08-21 | 2026-09-03 | 10 | +4.83% | FAIL |
| roll | 2026-08-24 | 2026-09-04 | 10 | -1.24% | FAIL |
| roll | 2026-08-25 | 2026-09-08 | 10 | -3.38% | FAIL |
| roll | 2026-08-26 | 2026-09-09 | 10 | -3.06% | FAIL |

## Day route

| Date | Score | Route | Equity | Cash | core | tac.mv |
|---|---:|---|---:|---:|---:|---:|
| 2026-08-13 | +8.53 | io | $99,830.06 | $9,917.52 | 9 | 0 |
| 2026-08-14 | +5.50 | io | $101,850.19 | $0.79 | 9 | 1 |
| 2026-08-17 | +2.25 | io | $103,218.85 | $1,246.69 | 18 | 0 |
| 2026-08-18 | -6.20 | hold | $103,416.36 | $93,954.35 | 9 | 0 |
| 2026-08-19 | -7.20 | hold | $103,598.34 | $93,954.35 | 9 | 0 |
| 2026-08-20 | +1.12 | mover | $105,475.82 | $4.87 | 0 | 11 |
| 2026-08-21 | +3.25 | mover | $111,436.33 | $1.37 | 0 | 11 |
| 2026-08-24 | -5.17 | hold | $112,522.35 | $112,522.35 | 0 | 0 |
| 2026-08-25 | +1.80 | io | $112,418.83 | $11,353.90 | 6 | 0 |
| 2026-08-26 | +2.02 | io | $112,045.44 | $1.32 | 6 | 2 |
| 2026-08-27 | — | io | $110,835.16 | $1,118.65 | 9 | 0 |
| 2026-08-28 | +0.75 | io | $108,477.87 | $9,711.68 | 4 | 2 |
| 2026-08-31 | -5.85 | hold | $110,611.29 | $10,787.03 | 4 | 0 |
| 2026-09-01 | -6.30 | hold | $113,268.79 | $21,087.18 | 1 | 0 |
| 2026-09-02 | -3.83 | hold | $116,939.41 | $116,939.41 | 0 | 0 |
| 2026-09-03 | -0.90 | io | $116,813.25 | $11,641.34 | 5 | 0 |
| 2026-09-04 | +2.25 | mover | $111,127.73 | $2.34 | 0 | 12 |
| 2026-09-08 | -11.47 | hold | $108,616.66 | $108,616.66 | 0 | 0 |
| 2026-09-09 | -13.95 | hold | $108,616.66 | $108,616.66 | 0 | 0 |

## Live method: 3d robust size book

`flatten_robust` is the production book. Selection is the 3d size sleeve (SLEEVE_COMBINE_BT's best .io hold), not raw `2w_size`. Names recycle every 3 sessions. Flatten → mover on a green morning with priced BUYs is unchanged. S ≤ −3 still blocks new tickets. `flatten_hard_red` is the old 2w_size live book (the 8-13 INO path).

| Book | Role | Return | Final | Max DD | min fortnight |
|---|---|---:|---:|---:|---:|
| `flatten_robust` | **LIVE** | +8.62% | $108,616.66 | 7.12% | -2.0 |

| Date | Score | Robust (live) | Hard-red 2w | Recycle |
|---|---:|---|---|---|
| 2026-08-13 | +8.53 | io $99,830 | — $0 | — $0 |
| 2026-08-14 | +5.50 | io $101,850 | — $0 | — $0 |
| 2026-08-17 | +2.25 | io $103,219 | — $0 | — $0 |
| 2026-08-18 | -6.20 | hold $103,416 | — $0 | — $0 |
| 2026-08-19 | -7.20 | hold $103,598 | — $0 | — $0 |
| 2026-08-20 | +1.12 | mover $105,476 | — $0 | — $0 |
| 2026-08-21 | +3.25 | mover $111,436 | — $0 | — $0 |
| 2026-08-24 | -5.17 | hold $112,522 | — $0 | — $0 |
| 2026-08-25 | +1.80 | io $112,419 | — $0 | — $0 |
| 2026-08-26 | +2.02 | io $112,045 | — $0 | — $0 |
| 2026-08-27 | — | io $110,835 | — $0 | — $0 |
| 2026-08-28 | +0.75 | io $108,478 | — $0 | — $0 |
| 2026-08-31 | -5.85 | hold $110,611 | — $0 | — $0 |
| 2026-09-01 | -6.30 | hold $113,269 | — $0 | — $0 |
| 2026-09-02 | -3.83 | hold $116,939 | — $0 | — $0 |
| 2026-09-03 | -0.90 | io $116,813 | — $0 | — $0 |
| 2026-09-04 | +2.25 | mover $111,128 | — $0 | — $0 |
| 2026-09-08 | -11.47 | hold $108,617 | — $0 | — $0 |
| 2026-09-09 | -13.95 | hold $108,617 | — $0 | — $0 |

## Sweep (same window, same fees)

| Policy | Return | Max DD | min fortnight | min block | Pass |
|---|---:|---:|---:|---:|---|
| `flatten_robust` | +8.62% | 7.12% | -2.0 | 12.24 | no |

## Why this merge

- **Mover** is the highest hit-rate sleeve on this tape (paper +9.3%, max DD 0.12%) because the S ≥ +1 gate deletes the fall days. It is *off* most sessions — that is the product, not a bug. The days it *is* on (08-20, 08-21) are the ones `.io` 2w_size lost or lagged.
- **.io `2w_size`** is the current top published book (+10.26%) and the one that keeps winning on SPY-down / hard-red mornings (08-14 +3.2%, 08-18/19 +2.0/+4.1%). An earlier NAV stitch that flattened on every green morning *including* 08-13/14 (zero BUY calls) sat in cash and gave the edge back.
- **Flatten, don't average.** Averaging pick lists re-imports Excel's median-zero payoff. The combined book *is* `.io` until a green morning that actually has a priced mover BUY list and a prior book, then it *is* mover for one session.
- **Open flatten is leak-free:** `.io` names were bought at a prior close; the 09:30 open is the first price you can get after the new morning predict. Today's book print is not known at 09:30 so the flatten uses yesterday / last print only. Tomorrow's score is never used at today's close.
- **Rotate at the next green open** is the honest way to stay fully invested in mover (the paper book's same-day close→open recycle is a leak; we do not copy it). Sells run first so the new list is funded with cash, not with stock you still hold. **Carry last book** is the same 2w_size list the .io dashboard already follows on a quiet print day — not a third model.

## Cash and fees (not a paper NAV)

This is one Futubull cash account. Every fill pays `00_grounding/futubull_fees.json` (commission + platform + settlement, plus SEC/TAF on sells). Equity = leftover cash + marked positions. Buying 2w_size names on 08-13 spends almost the whole $100k (day-end cash $136); those names stay held through 08-19, so the 08-14/17 add-ons are leftover crumbs (TBCH 1 share, VERI $15). On 08-20 the flatten sells first at the open (fees out), then mover buys consume that cash. On 08-24 the last 2w_size list is re-entered; 08-27's new book names only get the ~$186 leftover (CNH 3 shares, MOS 1 share) because the 08-24 lots are still open.

Nothing here is a fringe overlay: default = published `.io` `2w_size`, switch = published mover gate (S ≥ +1 and a real BUY list), flatten/rotate at the 09:30 print you already have, carry = keep the last 2w list. No NAV stitch, no same-day close→open recycle, no Excel vote, no leverage.

Code: `src/sleeve_merge.py`. Machine: `data/sleeve_merge/`. Dashboard: `dashboard/sleeve-merge/index.html`. Lookback (cameras / setups / 09:30 action): `dashboard/flatten-lookback/index.html`.
