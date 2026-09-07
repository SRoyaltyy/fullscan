# Full workbook (A–JO) → H/I mine

_Generated 2026-09-07. Research only. Live `flatten_robust` frozen.
Whole sheet, not A–O. Not #144’s hand-picked leftover list._

## Verdict (plain English)

We dumped **every letter** the emulator paints (A–JO, 275 columns,
Yahoo/rows seed — never Excel `--from-cache`) for **3,604** names and
mined **268 letters that actually printed** (255 numeric, 123 with
fills, 27 text). Same-row H and I were labels only. Every interesting
atom was also AND-ed with green O and the five-cell morning light, so
a second link could not hide as “only works next to O.”

It is **not** 272 independent edges.

The miner’s raw board on this latest tile (2026-01-08 → 2026-09-04,
**0 days in 2025**) was **KEEP 272 · KILL 3,866 · THIN 36**. After you
collapse duplicate thresholds (`eq1` = `ge1` = `gt0` on the same 0/1
flag) and formula twins (`S` is `DI` on daily rows, `V` is `DK`, `GH`
is `EC`, `JD` is yesterday’s `HO`), you get **two stories**:

1. **A washout flag, CE.** Yesterday the low was a *new* 24-day low
   that was also the 43-day low. Those names’ **next-day leftover H**
   averaged **+1.32%** after fees (n=2,563 holdout, 986 names, t=10.6)
   vs everyone **−0.09%**. That is the only same-day H family that
   looks like a real event on this tape. It is open-fair (lag 1). It
   has **not** been checked in 2025. It is a bounce-after-new-low,
   cousin to the A–F gap-fade, not a second live data vendor.
2. **A 2026 “beaten-up → next week up” sleeve.** `S`/`DI` red or low,
   `V`/`DK` low, `AN` = −1, `O` low, `CF` = 24-day low, `EC`/`GH` low.
   Those are composites of prior H/lows/volume. They print
   **+0.5% to +1.2%** over the next week vs a book that already made
   about **+0.08%**. Almost the entire KEEP list is this one bounce,
   reprinted 200 ways. Without 2025 this is a tape, not a card.

**#144’s morning five-cell light + green O did not reprint +2% H on
this dump.** Same-day H after fees was **−0.12%** (n=16,784). Do **not**
read that as “#144 is dead.” On AAPL the emulator paints **C mostly
orange** (score 0), and the five-cell light stays on **53 / 140** days
— far more often than #144’s ~1.3 hits per name. These ColorEngine
fills are not a pixel copy of the xlsx snapshot that mine used.

No other letter’s green fill, 0/1 flag, or text token produced a
second *morning-streak-shaped* same-day H edge on this tile.

## What we scanned

| | |
|---|---|
| Surface | A–JO (275). Formulas through JL. 268 letters present. |
| Seed | Yahoo / excel-state rows. Not `--from-cache`. |
| Names / liquid days | 3,251 / 332,341 |
| Window | 2026-01-08 → 2026-09-04. **2025 name-days: 0** (latest tile). |
| Gates | 7,368 single-letter + standing · 18,114 combo specs · 4,174 hardened |
| Clock | Lag ≥1 of any letter is open-fair. Lag 0 values = locked 44. Lag 0 fills = A,B,C,G,J,K,L,M,O (+IR/IS/IT). Same-day I is not an open label. |

Inventory: `FULL_GRID_INVENTORY.md`. Prior STEP tile (`--tile prev`) is
the 2025 check; it writes `TICKER__tprev.json` and does not overwrite
the latest dump.

## Standing family (must rediscover — did not, on this paint)

| recipe | same-day H holdout | verdict |
|---|---:|---|
| five-cell light (A,B,C,G,J scores, enter 5 / off 2) | −0.14% (n=36,239) | KILL |
| five-cell light + green O | **−0.12%** (n=16,784) | KILL |
| light+O + AH≥1 | −0.23% (n=3,116) | KILL |
| light+O + FR≥1 | −0.15% (n=9,997) | KILL |
| nine-cell light + green O | −0.09% (n=20,124) | KILL |

#144 published about **+2.1%** H on light+O. Here the light is a wide
regime, C’s fill is the wrong family, and leftover H is a scratch
after fees. Treat as **paint mismatch / tile mismatch**, not a
refutation of the xlsx-grid keep, until the same recipe is scored on
fills that match that snapshot.

## Same-day leftover H (the fair open question)

After collapsing twins, three open-clock gates cleared the 2026-only
ship bar (ticker holdout, both SPY tapes, Q1/Q3 when present, fees,
n + names, not a lottery):

| when (plain English) | H after fees | n | names | t | vs book |
|---|---:|---:|---:|---:|---:|
| **CE yesterday = 1** — yesterday’s low was a fresh 24-day low *and* the 43-day low (`CB=MIN(E,24d)`, `CC=MIN(E,43d)`, CE flips to 1 on that print) | **+1.32%** | 2,563 | 986 | 10.6 | −0.09% |
| HO two days ago = 1 (HN vs GU ±3% flip). Twin: JD yesterday = 1 | +0.21% | 4,684 | — | 2.3 | −0.09% |
| S two days ago = 1 AND AG yesterday = 1 | +0.32% | 842 | — | 3.2 | −0.09% |

`CE` same-day (close clock) is the same washout: next session’s I
+1.87%, next H +1.26%. You cannot know today’s CE at 09:30 — it reads
today’s low.

`CF` is the milder cousin: today’s low = 24-day low, no 43-day
requirement. It showed up in the 1-week bounce list, not as a
same-day H keep.

**CE is not a card.** 2026 only. Washout bounces die in other years.
The prior tile must keep +H vs the book in 2025 before this is even a
research keep.

## The 1-week bounce (why the KEEP count exploded)

Almost every other KEEP is “the sheet says this name is beaten up”
predicting the **next week**, on a 2026 tape where buy-everyone
already made about +0.08% after fees.

Twins of one composite:

| Excel letter | what it actually is |
|---|---|
| **S** (daily) | alias of **DI** — sum of flags on prior H, L, N, O, AA, AD, CP, DD, EL |
| **V** (daily) | alias of **DK** — count of “weak tape” conditions (H≤−3%, CP, EL, …) |
| **GH** | alias of **EC** — another signed score of volume/H/CP flags |
| **AN = −1** | recent FF=−1 and CP<0 |
| **O low** | the composite’s number, close-clock; lag-1 is fair |
| **CF = 1** | today’s low is a 24-day low |

Bottom-quintile / red-fill of those letters is the same sleeve. n is
often 24–30k of ~133k holdout days — just under the 25% “too wide”
cap. That is a factor, not a trigger.

## Spearman (lag-1 number, always open-fair)

Sparse letters (BE, BL, BX, GD; n of a few thousand) print |ρ|≈0.2
and are thin junk. On the fat tape:

| col | predicts | ρ holdout | n |
|---|---|---:|---:|
| CE | same-day H | +0.078 | 94,645 |
| CD | same-day H | −0.077 | 94,645 |
| GN / DX / GK / GO | next week I | ≈ −0.08 | ~120k |

CE’s continuous rank agrees with the binary flag. Nothing else in the
deep grid has a fat-n ρ that looks like a second vendor.

## Clock and algebra (unchanged)

Same-row H/I are never features. `I = gap + H×(1+gap)`. Same-day I is
not an open label. Close-lag0 features only predict **later** H/I.

A–F still come from Yahoo/rows.

## What would count as a second good link

A letter (or letter × O / light) whose **same-day leftover H** beats
the book after fees in **2025 and 2026**, both SPY tapes, n≥400, ≥50
names, not a lottery, not half the book — and is **not** just CE/CF
washout or the S/DI beaten-up twin.

On this latest tile, that second link is **not there**. The interesting
new object is CE’s washout, and it still owes 2025.

Research only. No cards. No live wire.
