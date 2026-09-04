# Combined sleeve — .io × mover

_Generated 2026-09-04T14:51:52 — 2026-08-13 → 2026-09-04_

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
| $100,000 | $116,477.72 | **+16.48%** | 3.64% | 39 | 48.7% | BEATS +12.85% | **FAIL** |

Futubull fees paid **$1,600.80** (in $787.55 / out $813.25). Whole shares. A name that is already held ties up cash — later names that day only see leftover cash, so some tickets do not fill.

| Side | Trades | Win | P&L |
|---|---:|---:|---:|
| BUY | 39 | 48.7% | $16,477.74 |
| SELL | 0 | 0.0% | $0.00 |

## 15% every 2 weeks

Target **+15%** per calendar fortnight (14 days) and per 10 trading sessions.
Fortnights: **FAIL** (min 10.46). 10-session blocks: **FAIL** (min 10.46). Rolling: **FAIL** (min 3.95).

| Kind | Start | End | n | Return | Gate |
|---|---|---|---:|---:|---|
| fortnight | 2026-08-13 | 2026-08-26 | 10 | +10.46% | FAIL |
| fortnight | 2026-08-27 | 2026-09-04 | 7 | +6.85% | partial |
| block | 2026-08-13 | 2026-08-26 | 10 | +10.46% | FAIL |
| block | 2026-08-27 | 2026-09-04 | 7 | +6.85% | partial |
| roll | 2026-08-13 | 2026-08-26 | 10 | +10.46% | FAIL |
| roll | 2026-08-14 | 2026-08-27 | 10 | +6.94% | FAIL |
| roll | 2026-08-17 | 2026-08-28 | 10 | +3.95% | FAIL |
| roll | 2026-08-18 | 2026-08-31 | 10 | +5.96% | FAIL |
| roll | 2026-08-19 | 2026-09-01 | 10 | +8.86% | FAIL |
| roll | 2026-08-20 | 2026-09-02 | 10 | +11.15% | FAIL |
| roll | 2026-08-21 | 2026-09-03 | 10 | +5.20% | FAIL |
| roll | 2026-08-24 | 2026-09-04 | 10 | +5.19% | FAIL |

## Day route

| Date | Score | Route | Equity | Cash | core | tac.mv |
|---|---:|---|---:|---:|---:|---:|
| 2026-08-13 | +8.53 | io | $99,811.65 | $9.99 | 9 | 0 |
| 2026-08-14 | +5.50 | io | $101,935.75 | $9.99 | 9 | 0 |
| 2026-08-17 | +2.25 | io | $102,651.45 | $9.99 | 9 | 0 |
| 2026-08-18 | -6.20 | hold | $102,916.72 | $102,916.72 | 0 | 0 |
| 2026-08-19 | -7.20 | hold | $102,916.72 | $102,916.72 | 0 | 0 |
| 2026-08-20 | +1.12 | mover | $104,792.81 | $203.54 | 0 | 10 |
| 2026-08-21 | +3.25 | mover | $110,715.55 | $161.07 | 0 | 10 |
| 2026-08-24 | -5.17 | hold | $110,735.60 | $110,735.60 | 0 | 0 |
| 2026-08-25 | +1.80 | io | $110,622.58 | $366.65 | 6 | 0 |
| 2026-08-26 | +2.02 | io | $110,249.65 | $366.65 | 6 | 0 |
| 2026-08-27 | — | io | $109,009.20 | $41.58 | 9 | 0 |
| 2026-08-28 | +0.75 | io | $106,702.82 | $23.07 | 4 | 0 |
| 2026-08-31 | -5.85 | hold | $109,052.82 | $23.07 | 4 | 0 |
| 2026-09-01 | -6.30 | hold | $112,030.53 | $347.07 | 1 | 0 |
| 2026-09-02 | -3.83 | hold | $116,477.72 | $116,477.72 | 0 | 0 |
| 2026-09-03 | -0.90 | io | $116,477.72 | $116,477.72 | 0 | 0 |
| 2026-09-04 | — | io | $116,477.72 | $116,477.72 | 0 | 0 |

## Live method: 3d robust size book

`flatten_robust` is the production book. Selection is the 3d size sleeve (SLEEVE_COMBINE_BT's best .io hold), not raw `2w_size`. Names recycle every 3 sessions. Flatten → mover on a green morning with priced BUYs is unchanged. S ≤ −3 still blocks new tickets. `flatten_hard_red` is the old 2w_size live book (the 8-13 INO path).

| Book | Role | Return | Final | Max DD | min fortnight |
|---|---|---:|---:|---:|---:|
| `flatten_robust` | **LIVE** | +16.48% | $116,477.72 | 3.64% | 10.46 |
| `flatten_hard_red` | previous 2w_size | +19.06% | $119,063.84 | 2.99% | 16.85 |
| `flatten_switch_recycle` | ungated predecessor | +21.57% | $121,571.23 | 2.97% | 19.13 |

| Date | Score | Robust (live) | Hard-red 2w | Recycle |
|---|---:|---|---|---|
| 2026-08-13 | +8.53 | io $99,812 | io $99,801 | io $99,801 |
| 2026-08-14 | +5.50 | io $101,936 | io $102,951 | io $102,951 |
| 2026-08-17 | +2.25 | io $102,651 | io $103,841 | io $103,841 |
| 2026-08-18 | -6.20 | hold $102,917 | hold $105,907 | io $105,907 |
| 2026-08-19 | -7.20 | hold $102,917 | hold $110,260 | io $110,260 |
| 2026-08-20 | +1.12 | mover $104,793 | mover $111,605 | mover $111,605 |
| 2026-08-21 | +3.25 | mover $110,716 | mover $117,913 | mover $117,913 |
| 2026-08-24 | -5.17 | hold $110,736 | hold $117,933 | io $117,848 |
| 2026-08-25 | +1.80 | io $110,623 | io $117,849 | io $120,173 |
| 2026-08-26 | +2.02 | io $110,250 | io $116,617 | io $118,894 |
| 2026-08-27 | — | io $109,009 | io $116,191 | io $118,485 |
| 2026-08-28 | +0.75 | io $106,703 | io $114,403 | io $116,609 |
| 2026-08-31 | -5.85 | hold $109,053 | hold $114,767 | io $116,992 |
| 2026-09-01 | -6.30 | hold $112,031 | hold $114,648 | io $116,857 |
| 2026-09-02 | -3.83 | hold $116,478 | hold $117,014 | io $119,328 |
| 2026-09-03 | -0.90 | io $116,478 | io $119,064 | io $121,571 |
| 2026-09-04 | — | io $116,478 | io $119,064 | io $121,571 |

## If you started any day

Fresh $100,000 each session, policy `flatten_robust`, through 2026-09-04. Mean **+5.27%** across 17 starts (11 finished above start). Starts with ≥5 sessions left: mean **6.89%** (n=13, min -2.25). Held stock ties up cash. Weekend dates have no tape.

| Start | First route | Would-buy if you are full of cash | Return | Sessions | Made money |
|---|---|---|---:|---:|---|
| 2026-08-13 | io | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | +16.48% | 17 | YES |
| 2026-08-14 | io | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT, BETR | +3.51% | 16 | YES |
| 2026-08-17 | io | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST, NB | +8.43% | 15 | YES |
| 2026-08-18 | hold | OXY, APA, COP, MUR, MLYS, TRMD, OBE, CYPH, TBPH | +13.18% | 14 | YES |
| 2026-08-19 | hold | OBE, STE, DHR, SYK, MUR, TRMD, MLYS, TBPH, INMD | +13.18% | 13 | YES |
| 2026-08-20 | mover | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS, AEM | +13.18% | 12 | YES |
| 2026-08-21 | mover | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH, FUTU, GMAB | +7.86% | 11 | YES |
| 2026-08-24 | hold | RZLT, MOS, OCUL, INSP, CRMD, HCA | +5.18% | 10 | YES |
| 2026-08-25 | io | MOS, OCUL, INSP, CRMD, RZLT, HCA | +5.18% | 9 | YES |
| 2026-08-26 | io | OCUL, CRMD, RZLT, MOS, INSP, HCA | -2.25% | 8 | no |
| 2026-08-27 | io | RRC, CRK, MOS, SLI | +2.18% | 7 | YES |
| 2026-08-28 | io | RRC, CRK, MOS, SLI | +3.40% | 6 | YES |
| 2026-08-31 | hold | RES, PBF, NOV, WTTR | +0.00% | 5 | no |
| 2026-09-01 | hold | DK, BTE, MTDR, RES, KOS, OIS, FTI, KMI, OKE | +0.00% | 4 | no |
| 2026-09-02 | hold | PCRX, HRMY, PBH, VSTM, MGTX, PBR-A, PBR | +0.00% | 3 | no |
| 2026-09-03 | io | ATRC, HRMY, CABA, VSTM, RVTY | +0.00% | 2 | no |
| 2026-09-04 | io | ASND, OSCR, ATRC, NVAX, CABA, BVS | +0.00% | 1 | no |

## Sweep (same window, same fees)

| Policy | Return | Max DD | min fortnight | min block | Pass |
|---|---:|---:|---:|---:|---|
| `flatten_switch_recycle` | +21.57% | 2.97% | 19.13 | 19.13 | YES |
| `flatten_rotate` | +21.99% | 2.30% | 18.17 | 18.17 | YES |
| `flatten_hard_red` | +19.06% | 2.99% | 16.85 | 16.85 | YES |
| `flatten_carry_book` | +18.55% | 2.97% | 16.16 | 16.16 | YES |
| `flatten_switch_full` | +18.95% | 2.30% | 15.22 | 15.22 | YES |
| `flatten_skip_blank_io` | +17.56% | 0.20% | 15.22 | 15.22 | YES |
| `flatten_cash_mover` | +18.38% | 2.76% | 14.67 | 14.67 | no |
| `flatten_blank_cash` | +17.84% | 0.48% | 14.67 | 14.67 | no |
| `flatten_switch_70` | +16.63% | 2.30% | 12.97 | 12.97 | no |
| `flatten_rich` | +15.69% | 2.30% | 12.06 | 12.06 | no |
| `flatten_overlap` | +15.09% | 2.30% | 11.49 | 11.49 | no |
| `flatten_overlap_55` | +15.00% | 2.30% | 11.41 | 11.41 | no |
| `flatten_switch_60` | +14.62% | 2.30% | 11.02 | 11.02 | no |
| `flatten_switch` | +14.58% | 2.30% | 10.99 | 10.99 | no |
| `flatten_robust` | +16.48% | 3.64% | 10.46 | 10.46 | no |
| `io_3d_switch` | +10.44% | 1.62% | 10.14 | 10.14 | no |
| `concentrated_switch` | +12.28% | 1.59% | 9.58 | 9.58 | no |
| `core50_switch` | +12.48% | 1.69% | 9.18 | 9.18 | no |
| `switch_70` | +11.31% | 1.68% | 8.82 | 8.82 | no |
| `core_switch` | +11.45% | 1.68% | 8.73 | 8.73 | no |
| `switch_80` | +11.05% | 1.60% | 8.67 | 8.67 | no |
| `switch_no_short` | +9.86% | 1.99% | 8.67 | 8.67 | no |
| `hard_red_shorts` | +8.87% | 2.57% | 7.83 | 7.83 | no |
| `switch_80_overlap` | +8.37% | 1.99% | 7.6 | 7.6 | no |
| `flatten_3d` | +10.97% | 2.30% | 7.49 | 7.49 | no |
| `mover_heavy` | +9.52% | 1.56% | 7.29 | 7.29 | no |
| `switch_90_overlap` | +7.23% | 1.96% | 6.89 | 6.89 | no |

## Why this merge

- **Mover** is the highest hit-rate sleeve on this tape (paper +9.3%, max DD 0.12%) because the S ≥ +1 gate deletes the fall days. It is *off* most sessions — that is the product, not a bug. The days it *is* on (08-20, 08-21) are the ones `.io` 2w_size lost or lagged.
- **.io `2w_size`** is the current top published book (+12.85%) and the one that keeps winning on SPY-down / hard-red mornings (08-14 +3.2%, 08-18/19 +2.0/+4.1%). An earlier NAV stitch that flattened on every green morning *including* 08-13/14 (zero BUY calls) sat in cash and gave the edge back.
- **Flatten, don't average.** Averaging pick lists re-imports Excel's median-zero payoff. The combined book *is* `.io` until a green morning that actually has a priced mover BUY list and a prior book, then it *is* mover for one session.
- **Open flatten is leak-free:** `.io` names were bought at a prior close; the 09:30 open is the first price you can get after the new morning predict. Today's book print is not known at 09:30 so the flatten uses yesterday / last print only. Tomorrow's score is never used at today's close.
- **Rotate at the next green open** is the honest way to stay fully invested in mover (the paper book's same-day close→open recycle is a leak; we do not copy it). Sells run first so the new list is funded with cash, not with stock you still hold. **Carry last book** is the same 2w_size list the .io dashboard already follows on a quiet print day — not a third model.

## Cash and fees (not a paper NAV)

This is one Futubull cash account. Every fill pays `00_grounding/futubull_fees.json` (commission + platform + settlement, plus SEC/TAF on sells). Equity = leftover cash + marked positions. Buying 2w_size names on 08-13 spends almost the whole $100k (day-end cash $136); those names stay held through 08-19, so the 08-14/17 add-ons are leftover crumbs (TBCH 1 share, VERI $15). On 08-20 the flatten sells first at the open (fees out), then mover buys consume that cash. On 08-24 the last 2w_size list is re-entered; 08-27's new book names only get the ~$186 leftover (CNH 3 shares, MOS 1 share) because the 08-24 lots are still open.

Nothing here is a fringe overlay: default = published `.io` `2w_size`, switch = published mover gate (S ≥ +1 and a real BUY list), flatten/rotate at the 09:30 print you already have, carry = keep the last 2w list. No NAV stitch, no same-day close→open recycle, no Excel vote, no leverage.

Code: `src/sleeve_merge.py`. Machine: `data/sleeve_merge/`. Dashboard: `dashboard/sleeve-merge/index.html`. Lookback (cameras / setups / 09:30 action): `dashboard/flatten-lookback/index.html`.
