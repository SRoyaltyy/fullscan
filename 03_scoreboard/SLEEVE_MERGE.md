# Combined sleeve — .io × mover

_Generated 2026-09-10T10:44:28 — 2026-08-13 → 2026-09-10_

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
| $100,000 | $108,616.66 | **+8.62%** | 7.12% | 72 | 45.8% | trails +10.64% | **FAIL** |

Futubull fees paid **$2,622.22** (in $1,294.73 / out $1,327.49). Whole shares. A name that is already held ties up cash — later names that day only see leftover cash, so some tickets do not fill.

| Side | Trades | Win | P&L |
|---|---:|---:|---:|
| BUY | 72 | 45.8% | $8,616.68 |
| SELL | 0 | 0.0% | $0.00 |

## 15% every 2 weeks

Target **+15%** per calendar fortnight (14 days) and per 10 trading sessions.
Fortnights: **FAIL** (min -2.0). 10-session blocks: **FAIL** (min -2.0). Rolling: **FAIL** (min -3.38).

| Kind | Start | End | n | Return | Gate |
|---|---|---|---:|---:|---|
| fortnight | 2026-08-13 | 2026-08-26 | 10 | +12.24% | FAIL |
| fortnight | 2026-08-27 | 2026-09-09 | 9 | -2.00% | FAIL |
| fortnight | 2026-09-10 | 2026-09-10 | 1 | +0.00% | partial |
| block | 2026-08-13 | 2026-08-26 | 10 | +12.24% | FAIL |
| block | 2026-08-27 | 2026-09-10 | 10 | -2.00% | FAIL |
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
| roll | 2026-08-27 | 2026-09-10 | 10 | -2.00% | FAIL |

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
| 2026-09-10 | — | io | $108,616.66 | $108,616.66 | 0 | 0 |

## Live method: 3d robust size book

`flatten_robust` is the production book. Selection is the 3d size sleeve (SLEEVE_COMBINE_BT's best .io hold), not raw `2w_size`. Names recycle every 3 sessions. Flatten → mover on a green morning with priced BUYs is unchanged. S ≤ −3 still blocks new tickets. `flatten_hard_red` is the old 2w_size live book (the 8-13 INO path).

| Book | Role | Return | Final | Max DD | min fortnight |
|---|---|---:|---:|---:|---:|
| `flatten_robust` | **LIVE** | +8.62% | $108,616.66 | 7.12% | -2.0 |
| `flatten_hard_red` | previous 2w_size | +10.77% | $110,768.06 | 7.87% | -5.53 |
| `flatten_switch_recycle` | ungated predecessor | +13.80% | $113,795.89 | 8.03% | -4.87 |

| Date | Score | Robust (live) | Hard-red 2w | Recycle |
|---|---:|---|---|---|
| 2026-08-13 | +8.53 | io $99,830 | io $99,801 | io $99,801 |
| 2026-08-14 | +5.50 | io $101,850 | io $102,951 | io $102,951 |
| 2026-08-17 | +2.25 | io $103,219 | io $103,841 | io $103,841 |
| 2026-08-18 | -6.20 | hold $103,416 | hold $105,907 | io $105,907 |
| 2026-08-19 | -7.20 | hold $103,598 | hold $110,260 | io $110,260 |
| 2026-08-20 | +1.12 | mover $105,476 | mover $111,605 | mover $111,605 |
| 2026-08-21 | +3.25 | mover $111,436 | mover $117,913 | mover $117,913 |
| 2026-08-24 | -5.17 | hold $112,522 | hold $119,066 | io $118,980 |
| 2026-08-25 | +1.80 | io $112,419 | io $118,981 | io $121,326 |
| 2026-08-26 | +2.02 | io $112,045 | io $117,734 | io $120,034 |
| 2026-08-27 | — | io $110,835 | io $117,308 | io $119,623 |
| 2026-08-28 | +0.75 | io $108,478 | io $115,498 | io $117,729 |
| 2026-08-31 | -5.85 | hold $110,611 | hold $115,869 | io $118,116 |
| 2026-09-01 | -6.30 | hold $113,269 | hold $115,751 | io $117,984 |
| 2026-09-02 | -3.83 | hold $116,939 | hold $118,147 | io $120,484 |
| 2026-09-03 | -0.90 | io $116,813 | io $120,231 | io $122,744 |
| 2026-09-04 | +2.25 | mover $111,128 | mover $113,391 | mover $115,741 |
| 2026-09-08 | -11.47 | hold $108,617 | hold $110,826 | io $112,884 |
| 2026-09-09 | -13.95 | hold $108,617 | hold $110,826 | io $113,796 |
| 2026-09-10 | — | io $108,617 | io $110,768 | io $113,796 |

## If you started any day

Fresh $100,000 each session, policy `flatten_robust`, through 2026-09-10. Mean **-1.42%** across 20 starts (6 finished above start). Starts with ≥5 sessions left: mean **-1.38%** (n=16, min -8.91). Held stock ties up cash. Weekend dates have no tape.

| Start | First route | Would-buy if you are full of cash | Return | Sessions | Made money |
|---|---|---|---:|---:|---|
| 2026-08-13 | io | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | +8.62% | 20 | YES |
| 2026-08-14 | io | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, WWW | +2.93% | 19 | YES |
| 2026-08-17 | io | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST, NB, CDNL, ABX | +5.88% | 18 | YES |
| 2026-08-18 | hold | OXY, APA, COP, MUR, MLYS, TRMD, OBE, CYPH, TBPH, JLHL, CBRS | +4.87% | 17 | YES |
| 2026-08-19 | hold | OBE, STE, DHR, SYK, MUR, TRMD, MLYS, TBPH, INMD, GUTS, DUOL | +4.87% | 16 | YES |
| 2026-08-20 | mover | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS, AEM, MRVI, DNA | +4.87% | 15 | YES |
| 2026-08-21 | mover | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH, FUTU, GMAB, BTBT, ENHA | -0.07% | 14 | no |
| 2026-08-24 | hold | RZLT, MOS, OCUL, INSP, CRMD, HCA, CRML, SAFX | -3.48% | 13 | no |
| 2026-08-25 | io | MOS, OCUL, INSP, CRMD, RZLT, HCA, CAPR, SAFX | -3.48% | 12 | no |
| 2026-08-26 | io | OCUL, CRMD, RZLT, MOS, INSP, HCA, AVBP, FLNC | -8.91% | 11 | no |
| 2026-08-27 | io | RRC, CRK, MOS, SLI | -5.51% | 10 | no |
| 2026-08-28 | io | RRC, CRK, MOS, SLI, SEDG, GRRR | -4.27% | 9 | no |
| 2026-08-31 | hold | RES, PBF, NOV, WTTR, USDE, ACDC | -7.11% | 8 | no |
| 2026-09-01 | hold | DK, BTE, MTDR, RES, KOS, OIS, FTI, KMI, OKE, TRGP, AME | -7.11% | 7 | no |
| 2026-09-02 | hold | PCRX, HRMY, PBH, VSTM, MGTX, PBR-A, PBR, PRQR, FATE | -7.11% | 6 | no |
| 2026-09-03 | io | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MRNA | -7.11% | 5 | no |
| 2026-09-04 | mover | CABA, ALEC, BHC, BMEA, OABI, OPK, VIR, ATRC, CRM, HRMY, HQ, EOSE | -6.31% | 4 | no |
| 2026-09-08 | hold | GGB, SID, SUZ, TX, CAN, ABTC | +0.00% | 3 | no |
| 2026-09-09 | hold | PCG, AR, CIG, UGP, VET, GRNT, VENU, CABA | +0.00% | 2 | no |
| 2026-09-10 | io | LBRT, UROY, GPRK, MTSI, NVT, NU, ODD, SIG | +0.00% | 1 | no |

## Sweep (same window, same fees)

| Policy | Return | Max DD | min fortnight | min block | Pass |
|---|---:|---:|---:|---:|---|
| `flatten_switch` | +9.94% | 4.82% | -0.64 | -0.64 | no |
| `flatten_3d` | +6.48% | 4.82% | -0.64 | -0.64 | no |
| `flatten_switch_60` | +9.16% | 5.53% | -1.37 | -1.37 | no |
| `flatten_overlap_55` | +9.50% | 5.55% | -1.41 | -1.41 | no |
| `flatten_overlap` | +9.51% | 5.61% | -1.47 | -1.47 | no |
| `flatten_rich` | +9.58% | 6.04% | -1.91 | -1.91 | no |
| `core50_switch` | +5.93% | 4.09% | -1.95 | -1.95 | no |
| `flatten_robust` | +8.62% | 7.12% | -2.0 | -2.0 | no |
| `flatten_robust_ripper` | +8.62% | 7.12% | -2.0 | -2.0 | no |
| `core_switch` | +5.16% | 3.89% | -2.2 | -2.2 | no |
| `hard_red_shorts` | +1.88% | 6.26% | -2.7 | -2.7 | no |
| `flatten_switch_full` | +11.11% | 7.34% | -3.27 | -3.27 | no |
| `flatten_rotate` | +15.04% | 7.34% | -3.28 | -3.28 | no |
| `flatten_cash_mover` | +10.66% | 7.34% | -3.28 | -3.28 | no |
| `switch_70` | +4.03% | 4.79% | -3.29 | -3.29 | no |
| `flatten_switch_70` | +8.85% | 7.42% | -3.35 | -3.35 | no |
| `switch_80` | +3.31% | 5.38% | -3.65 | -3.65 | no |
| `flatten_skip_blank_io` | +10.53% | 6.74% | -3.88 | -3.88 | no |
| `mover_heavy` | +1.31% | 5.90% | -4.02 | -4.02 | no |
| `concentrated_switch` | +3.60% | 5.80% | -4.17 | -4.17 | no |
| `io_3d_switch` | +4.07% | 5.79% | -4.24 | -4.24 | no |
| `switch_no_short` | +3.05% | 6.11% | -4.53 | -4.53 | no |
| `flatten_switch_recycle` | +13.80% | 8.03% | -4.87 | -4.87 | no |
| `flatten_carry_book` | +9.91% | 8.03% | -4.87 | -4.87 | no |
| `switch_80_overlap` | +1.09% | 6.77% | -5.24 | -5.24 | no |
| `flatten_hard_red` | +10.77% | 7.87% | -5.53 | -5.58 | no |
| `flatten_blank_cash` | +9.34% | 7.08% | -5.97 | -5.97 | no |
| `switch_90_overlap` | -0.86% | 7.87% | -6.27 | -6.27 | no |

## Why this merge

- **Mover** is the highest hit-rate sleeve on this tape (paper +9.3%, max DD 0.12%) because the S ≥ +1 gate deletes the fall days. It is *off* most sessions — that is the product, not a bug. The days it *is* on (08-20, 08-21) are the ones `.io` 2w_size lost or lagged.
- **.io `2w_size`** is the current top published book (+10.64%) and the one that keeps winning on SPY-down / hard-red mornings (08-14 +3.2%, 08-18/19 +2.0/+4.1%). An earlier NAV stitch that flattened on every green morning *including* 08-13/14 (zero BUY calls) sat in cash and gave the edge back.
- **Flatten, don't average.** Averaging pick lists re-imports Excel's median-zero payoff. The combined book *is* `.io` until a green morning that actually has a priced mover BUY list and a prior book, then it *is* mover for one session.
- **Open flatten is leak-free:** `.io` names were bought at a prior close; the 09:30 open is the first price you can get after the new morning predict. Today's book print is not known at 09:30 so the flatten uses yesterday / last print only. Tomorrow's score is never used at today's close.
- **Rotate at the next green open** is the honest way to stay fully invested in mover (the paper book's same-day close→open recycle is a leak; we do not copy it). Sells run first so the new list is funded with cash, not with stock you still hold. **Carry last book** is the same 2w_size list the .io dashboard already follows on a quiet print day — not a third model.

## Cash and fees (not a paper NAV)

This is one Futubull cash account. Every fill pays `00_grounding/futubull_fees.json` (commission + platform + settlement, plus SEC/TAF on sells). Equity = leftover cash + marked positions. Buying 2w_size names on 08-13 spends almost the whole $100k (day-end cash $136); those names stay held through 08-19, so the 08-14/17 add-ons are leftover crumbs (TBCH 1 share, VERI $15). On 08-20 the flatten sells first at the open (fees out), then mover buys consume that cash. On 08-24 the last 2w_size list is re-entered; 08-27's new book names only get the ~$186 leftover (CNH 3 shares, MOS 1 share) because the 08-24 lots are still open.

Nothing here is a fringe overlay: default = published `.io` `2w_size`, switch = published mover gate (S ≥ +1 and a real BUY list), flatten/rotate at the 09:30 print you already have, carry = keep the last 2w list. No NAV stitch, no same-day close→open recycle, no Excel vote, no leverage.

Code: `src/sleeve_merge.py`. Machine: `data/sleeve_merge/`. Dashboard: `dashboard/sleeve-merge/index.html`. Lookback (cameras / setups / 09:30 action): `dashboard/flatten-lookback/index.html`.
