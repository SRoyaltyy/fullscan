# Combined sleeve — .io × mover

_Generated 2026-09-09T04:58:47 — 2026-08-13 → 2026-09-08_

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
| $100,000 | $115,409.94 | **+15.41%** | 3.47% | 61 | 50.8% | BEATS +7.57% | **FAIL** |

Futubull fees paid **$1,902.30** (in $937.73 / out $964.57). Whole shares. A name that is already held ties up cash — later names that day only see leftover cash, so some tickets do not fill.

| Side | Trades | Win | P&L |
|---|---:|---:|---:|
| BUY | 61 | 50.8% | $15,409.90 |
| SELL | 0 | 0.0% | $0.00 |

## 15% every 2 weeks

Target **+15%** per calendar fortnight (14 days) and per 10 trading sessions.
Fortnights: **FAIL** (min 10.73). 10-session blocks: **FAIL** (min 10.73). Rolling: **FAIL** (min 4.23).

| Kind | Start | End | n | Return | Gate |
|---|---|---|---:|---:|---|
| fortnight | 2026-08-13 | 2026-08-26 | 10 | +10.73% | FAIL |
| fortnight | 2026-08-27 | 2026-09-08 | 9 | +5.49% | partial |
| block | 2026-08-13 | 2026-08-26 | 10 | +10.73% | FAIL |
| block | 2026-08-27 | 2026-09-08 | 9 | +5.49% | partial |
| roll | 2026-08-13 | 2026-08-26 | 10 | +10.73% | FAIL |
| roll | 2026-08-14 | 2026-08-27 | 10 | +7.57% | FAIL |
| roll | 2026-08-17 | 2026-08-28 | 10 | +4.68% | FAIL |
| roll | 2026-08-18 | 2026-08-31 | 10 | +6.91% | FAIL |
| roll | 2026-08-19 | 2026-09-01 | 10 | +9.35% | FAIL |
| roll | 2026-08-20 | 2026-09-02 | 10 | +11.17% | FAIL |
| roll | 2026-08-21 | 2026-09-03 | 10 | +5.00% | FAIL |
| roll | 2026-08-24 | 2026-09-04 | 10 | +4.23% | FAIL |
| roll | 2026-08-25 | 2026-09-07 | 10 | +4.33% | FAIL |
| roll | 2026-08-26 | 2026-09-08 | 10 | +4.40% | FAIL |

## Day route

| Date | Score | Route | Equity | Cash | core | tac.mv |
|---|---:|---|---:|---:|---:|---:|
| 2026-08-13 | +8.53 | io | $99,830.06 | $9,917.52 | 9 | 0 |
| 2026-08-14 | +5.50 | io | $101,706.21 | $1,806.16 | 18 | 0 |
| 2026-08-17 | +2.25 | io | $102,335.77 | $522.53 | 26 | 0 |
| 2026-08-18 | -6.20 | hold | $102,172.12 | $93,230.19 | 17 | 0 |
| 2026-08-19 | -7.20 | hold | $102,317.02 | $101,031.23 | 8 | 0 |
| 2026-08-20 | +1.12 | mover | $103,922.34 | $43.38 | 0 | 10 |
| 2026-08-21 | +3.25 | mover | $109,917.38 | $24.78 | 0 | 10 |
| 2026-08-24 | -5.17 | hold | $110,984.65 | $110,984.65 | 0 | 0 |
| 2026-08-25 | +1.80 | io | $110,882.53 | $11,423.40 | 6 | 0 |
| 2026-08-26 | +2.02 | io | $110,545.82 | $11,423.40 | 6 | 0 |
| 2026-08-27 | — | io | $109,407.76 | $1,139.12 | 9 | 0 |
| 2026-08-28 | +0.75 | io | $107,129.12 | $9,684.84 | 4 | 0 |
| 2026-08-31 | -5.85 | hold | $109,231.73 | $9,684.84 | 4 | 0 |
| 2026-09-01 | -6.30 | hold | $111,880.34 | $20,119.99 | 1 | 0 |
| 2026-09-02 | -3.83 | hold | $115,534.19 | $115,534.19 | 0 | 0 |
| 2026-09-03 | -0.90 | io | $115,409.52 | $11,513.75 | 5 | 0 |
| 2026-09-04 | — | io | $115,679.10 | $11,513.75 | 5 | 0 |
| 2026-09-07 | — | io | $115,679.10 | $11,513.75 | 5 | 0 |
| 2026-09-08 | — | io | $115,409.94 | $115,409.94 | 0 | 0 |

## Live method: 3d robust size book

`flatten_robust` is the production book. Selection is the 3d size sleeve (SLEEVE_COMBINE_BT's best .io hold), not raw `2w_size`. Names recycle every 3 sessions. Flatten → mover on a green morning with priced BUYs is unchanged. S ≤ −3 still blocks new tickets. `flatten_hard_red` is the old 2w_size live book (the 8-13 INO path).

| Book | Role | Return | Final | Max DD | min fortnight |
|---|---|---:|---:|---:|---:|
| `flatten_robust` | **LIVE** | +15.41% | $115,409.94 | 3.47% | 10.73 |
| `flatten_hard_red` | previous 2w_size | +17.99% | $117,987.36 | 3.00% | 17.98 |
| `flatten_switch_recycle` | ungated predecessor | +20.45% | $120,451.57 | 2.96% | 20.29 |

| Date | Score | Robust (live) | Hard-red 2w | Recycle |
|---|---:|---|---|---|
| 2026-08-13 | +8.53 | io $99,830 | io $99,801 | io $99,801 |
| 2026-08-14 | +5.50 | io $101,706 | io $102,951 | io $102,951 |
| 2026-08-17 | +2.25 | io $102,336 | io $103,841 | io $103,841 |
| 2026-08-18 | -6.20 | hold $102,172 | hold $105,907 | io $105,907 |
| 2026-08-19 | -7.20 | hold $102,317 | hold $110,260 | io $110,260 |
| 2026-08-20 | +1.12 | mover $103,922 | mover $111,504 | mover $111,504 |
| 2026-08-21 | +3.25 | mover $109,917 | mover $117,928 | mover $117,928 |
| 2026-08-24 | -5.17 | hold $110,985 | hold $119,081 | io $118,995 |
| 2026-08-25 | +1.80 | io $110,883 | io $118,996 | io $121,340 |
| 2026-08-26 | +2.02 | io $110,546 | io $117,749 | io $120,049 |
| 2026-08-27 | — | io $109,408 | io $117,323 | io $119,637 |
| 2026-08-28 | +0.75 | io $107,129 | io $115,511 | io $117,743 |
| 2026-08-31 | -5.85 | hold $109,232 | hold $115,880 | io $118,131 |
| 2026-09-01 | -6.30 | hold $111,880 | hold $115,760 | io $117,999 |
| 2026-09-02 | -3.83 | hold $115,534 | hold $118,154 | io $120,499 |
| 2026-09-03 | -0.90 | io $115,410 | io $120,242 | io $122,759 |
| 2026-09-04 | — | io $115,679 | io $119,022 | io $121,500 |
| 2026-09-07 | — | io $115,679 | io $119,022 | io $121,500 |
| 2026-09-08 | — | io $115,410 | io $117,987 | io $120,452 |

## If you started any day

Fresh $100,000 each session, policy `flatten_robust`, through 2026-09-08. Mean **+5.01%** across 19 starts (11 finished above start). Starts with ≥5 sessions left: mean **6.35%** (n=15, min -2.01). Held stock ties up cash. Weekend dates have no tape.

| Start | First route | Would-buy if you are full of cash | Return | Sessions | Made money |
|---|---|---|---:|---:|---|
| 2026-08-13 | io | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | +15.41% | 19 | YES |
| 2026-08-14 | io | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT, BETR | +9.35% | 18 | YES |
| 2026-08-17 | io | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST, NB | +13.96% | 17 | YES |
| 2026-08-18 | hold | OXY, APA, COP, MUR, MLYS, TRMD, OBE, CYPH, TBPH | +12.81% | 16 | YES |
| 2026-08-19 | hold | OBE, STE, DHR, SYK, MUR, TRMD, MLYS, TBPH, INMD | +12.81% | 15 | YES |
| 2026-08-20 | mover | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS, AEM | +12.81% | 14 | YES |
| 2026-08-21 | mover | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH, FUTU, GMAB | +7.64% | 13 | YES |
| 2026-08-24 | hold | RZLT, MOS, OCUL, INSP, CRMD, HCA | +3.99% | 12 | YES |
| 2026-08-25 | io | MOS, OCUL, INSP, CRMD, RZLT, HCA | +3.99% | 11 | YES |
| 2026-08-26 | io | OCUL, CRMD, RZLT, MOS, INSP, HCA | -2.01% | 10 | no |
| 2026-08-27 | io | RRC, CRK, MOS, SLI | +1.84% | 9 | YES |
| 2026-08-28 | io | RRC, CRK, MOS, SLI | +2.94% | 8 | YES |
| 2026-08-31 | hold | RES, PBF, NOV, WTTR | -0.11% | 7 | no |
| 2026-09-01 | hold | DK, BTE, MTDR, RES, KOS, OIS, FTI, KMI, OKE | -0.11% | 6 | no |
| 2026-09-02 | hold | PCRX, HRMY, PBH, VSTM, MGTX, PBR-A, PBR | -0.11% | 5 | no |
| 2026-09-03 | io | ATRC, HRMY, CABA, VSTM, RVTY | -0.11% | 4 | no |
| 2026-09-04 | io | ASND, OSCR, ATRC, NVAX, CABA, BVS | +0.00% | 3 | no |
| 2026-09-07 | io | BSX, UGP, LPG, CABA, NVAX, ALT | +0.00% | 2 | no |
| 2026-09-08 | io | SUZ, TX, GGB, SID | +0.00% | 1 | no |

## Sweep (same window, same fees)

| Policy | Return | Max DD | min fortnight | min block | Pass |
|---|---:|---:|---:|---:|---|
| `flatten_switch_recycle` | +20.45% | 2.96% | 20.29 | 20.29 | YES |
| `flatten_rotate` | +22.83% | 2.30% | 19.32 | 19.32 | YES |
| `flatten_hard_red` | +17.99% | 3.00% | 17.98 | 17.98 | YES |
| `flatten_carry_book` | +16.21% | 2.97% | 16.06 | 16.06 | YES |
| `flatten_skip_blank_io` | +18.92% | 0.20% | 15.12 | 15.12 | YES |
| `flatten_switch_full` | +18.51% | 2.30% | 15.12 | 15.12 | YES |
| `flatten_cash_mover` | +18.04% | 4.13% | 14.66 | 14.66 | no |
| `flatten_blank_cash` | +17.65% | 2.62% | 14.66 | 14.66 | no |
| `flatten_switch_70` | +16.17% | 2.30% | 12.84 | 12.84 | no |
| `flatten_rich` | +15.29% | 2.30% | 11.98 | 11.98 | no |
| `flatten_overlap` | +14.68% | 2.30% | 11.4 | 11.4 | no |
| `flatten_overlap_55` | +14.56% | 2.30% | 11.27 | 11.27 | no |
| `flatten_switch` | +14.12% | 2.30% | 10.86 | 10.86 | no |
| `flatten_switch_60` | +14.10% | 2.30% | 10.84 | 10.84 | no |
| `flatten_robust` | +15.41% | 3.47% | 10.73 | 10.73 | no |
| `flatten_robust_ripper` | +15.41% | 3.47% | 10.73 | 10.73 | no |
| `io_3d_switch` | +7.91% | 1.77% | 8.85 | 8.85 | no |
| `core50_switch` | +11.08% | 1.58% | 7.77 | 7.77 | no |
| `flatten_3d` | +10.51% | 2.37% | 7.33 | 7.33 | no |
| `concentrated_switch` | +9.71% | 1.52% | 7.27 | 7.27 | no |
| `core_switch` | +9.52% | 1.60% | 6.95 | 6.95 | no |
| `switch_70` | +9.06% | 1.62% | 6.83 | 6.83 | no |
| `switch_no_short` | +7.72% | 1.92% | 6.76 | 6.76 | no |
| `switch_80` | +8.35% | 1.56% | 6.33 | 6.33 | no |
| `hard_red_shorts` | +6.64% | 2.47% | 5.83 | 5.83 | no |
| `switch_80_overlap` | +5.94% | 1.94% | 5.51 | 5.51 | no |
| `mover_heavy` | +6.82% | 1.56% | 5.0 | 5.0 | no |
| `switch_90_overlap` | +4.47% | 1.97% | 4.56 | 4.56 | no |

## Why this merge

- **Mover** is the highest hit-rate sleeve on this tape (paper +9.3%, max DD 0.12%) because the S ≥ +1 gate deletes the fall days. It is *off* most sessions — that is the product, not a bug. The days it *is* on (08-20, 08-21) are the ones `.io` 2w_size lost or lagged.
- **.io `2w_size`** is the current top published book (+7.57%) and the one that keeps winning on SPY-down / hard-red mornings (08-14 +3.2%, 08-18/19 +2.0/+4.1%). An earlier NAV stitch that flattened on every green morning *including* 08-13/14 (zero BUY calls) sat in cash and gave the edge back.
- **Flatten, don't average.** Averaging pick lists re-imports Excel's median-zero payoff. The combined book *is* `.io` until a green morning that actually has a priced mover BUY list and a prior book, then it *is* mover for one session.
- **Open flatten is leak-free:** `.io` names were bought at a prior close; the 09:30 open is the first price you can get after the new morning predict. Today's book print is not known at 09:30 so the flatten uses yesterday / last print only. Tomorrow's score is never used at today's close.
- **Rotate at the next green open** is the honest way to stay fully invested in mover (the paper book's same-day close→open recycle is a leak; we do not copy it). Sells run first so the new list is funded with cash, not with stock you still hold. **Carry last book** is the same 2w_size list the .io dashboard already follows on a quiet print day — not a third model.

## Cash and fees (not a paper NAV)

This is one Futubull cash account. Every fill pays `00_grounding/futubull_fees.json` (commission + platform + settlement, plus SEC/TAF on sells). Equity = leftover cash + marked positions. Buying 2w_size names on 08-13 spends almost the whole $100k (day-end cash $136); those names stay held through 08-19, so the 08-14/17 add-ons are leftover crumbs (TBCH 1 share, VERI $15). On 08-20 the flatten sells first at the open (fees out), then mover buys consume that cash. On 08-24 the last 2w_size list is re-entered; 08-27's new book names only get the ~$186 leftover (CNH 3 shares, MOS 1 share) because the 08-24 lots are still open.

Nothing here is a fringe overlay: default = published `.io` `2w_size`, switch = published mover gate (S ≥ +1 and a real BUY list), flatten/rotate at the 09:30 print you already have, carry = keep the last 2w list. No NAV stitch, no same-day close→open recycle, no Excel vote, no leverage.

Code: `src/sleeve_merge.py`. Machine: `data/sleeve_merge/`. Dashboard: `dashboard/sleeve-merge/index.html`. Lookback (cameras / setups / 09:30 action): `dashboard/flatten-lookback/index.html`.
