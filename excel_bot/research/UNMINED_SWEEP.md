# Remaining A–JL families — unmined sweep

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge._

## Plain English

Same four leftover families as the 400-name cut (open leftover numbers, close leftover numbers, leftover green/red highlights, 0/1 formula flags). This beat rebuilds A–JL from **every Yahoo/rows cache** (~3,603 names), not a new tiny hand list. Never Excel's STOCKHISTORY cache. Fees are Futubull. We only buy at the open when the sheet already knows the number or the color at 9:30; everything else waits for the close.

A keeper has to work on both ticker halves, both calendar halves (cut 2026-05-01), **Q3** (cut 2026-07-01), both SPY tapes, and must beat 'just buy everyone' by 20 bps. The fattest single day cannot be more than 25% of winning-day P&L. hold1 needs a hold2 sibling; hold5 needs hold2 edge.

Full rows-cache rebuild **3603** tickers (2137 discovery / 1461 holdout) · lean capture **1.2 s/ticker** · specs **2207** · scored cells **3031**.

**KEEP 126 · KILL 2890 · THIN 15** (raw PASS 126 / FAIL 2890 / THIN 15).

Live cards stay frozen. Light+green O remain the only standing A–O color keeps. Finviz volume stays BLOCKED. AB / weather / book stay dead. Leftover-family KEEPs below are research-only on this 2026 tape.

### Family scoreboard

| family | what it is | KEEP | KILL | THIN |
|---|---|---:|---:|---:|
| `val_open` | open-knowable numbers past A/C/J | 10 | 356 | 3 |
| `val_close` | close-knowable leftover numbers | 99 | 2104 | 12 |
| `fill_new` | leftover green/red highlights | 14 | 286 | 0 |
| `flag` | 0/1 formula flags | 3 | 144 | 0 |

### Unconditional baseline (this sample, Futubull)

| clock | side | hold | n | avg net | t | win |
|---|---|---:|---:|---:|---:|---:|
| open | long | 1 | 502208 | -0.04% | -0.9 | 45% |
| open | long | 2 | 498610 | +0.54% | 5.1 | 46% |
| open | long | 5 | 487816 | +1.45% | 10.9 | 47% |
| open | short | 1 | 502208 | -0.31% | -6.0 | 46% |
| open | short | 2 | 498610 | -0.89% | -8.4 | 47% |
| open | short | 5 | 487816 | -1.80% | -13.5 | 49% |
| close | long | 1 | 498610 | +0.48% | 5.2 | 45% |
| close | long | 2 | 495012 | +0.83% | 7.6 | 46% |
| close | long | 5 | 484218 | +1.71% | 13.6 | 47% |
| close | short | 1 | 498610 | -0.83% | -9.1 | 46% |
| close | short | 2 | 495012 | -1.18% | -10.9 | 47% |
| close | short | 5 | 484218 | -2.06% | -16.4 | 49% |

### KEEP (hardened, unique letters)

Raw KEEP **126** collapses to **66** letter×hold cells (**40** hold1/2) on letters `AH/AM/BA/BN/CJ/CZ/EH/EJ/EY/FC/FK/FL/FR/GV/HO/HZ/IB/IK/IL/N/R/T/U`. eq1/ge1/gt0 twins that fire the same days are counted once. These cleared the same bar as light+O (both halves, Q3, both tapes, top-day lottery, beat baseline, horizon sibling). Research only — one 2026 regime. No card.

| letter | what it is | clock | hold | disc | holdout | Q3 | vs everyone | day-lottery | def |
|---|---|---|---|---|---|---|---|---|---|
| **EH** | any of FP–FU is negative → 4, else 0 | close | hold2 | 180453/+1.31%/t=6.7 | 124724/+1.34%/t=5.4 | 116468/+2.74%/t=7.3 | +0.51 pp | 9.4% | `valclose_EH_ge1` |
| **BA** | a 0/1 stress flag (HN/CP) | close | hold2 | 111276/+2.06%/t=6.5 | 75291/+1.96%/t=5.0 | 69356/+4.42%/t=7.1 | +1.13 pp | 10.2% | `valclose_BA_eq1` |
| **CZ** | how many recent CP prints were negative | close | hold2 | 192876/+1.30%/t=7.0 | 131044/+1.51%/t=5.0 | 123062/+2.91%/t=7.0 | +0.68 pp | 8.7% | `valclose_CZ_ge2` |
| **FC** | carried-forward FA state | close | hold2 | 172664/+1.44%/t=6.9 | 117363/+1.65%/t=4.8 | 128649/+2.77%/t=7.0 | +0.82 pp | 9.0% | `valclose_FC_ge1` |
| **IK** | carried-forward HO | close | hold2 | 83091/+1.86%/t=5.7 | 54754/+2.16%/t=4.6 | 54787/+3.86%/t=6.2 | +1.33 pp | 8.4% | `valclose_IK_eq1` |
| **HZ** | carried-forward HT | close | hold2 | 221745/+1.09%/t=6.7 | 151661/+1.20%/t=4.6 | 159892/+2.20%/t=6.9 | +0.37 pp | 8.9% | `valclose_HZ_ge1` |
| **BN** | today's volume vs a ~50-day average | close | hold2 | 189800/+1.19%/t=6.3 | 129756/+1.39%/t=4.5 | 160019/+2.21%/t=6.9 | +0.56 pp | 10.0% | `valclose_BN_gt0` |
| **CJ** | a running price sum / 50 | close | hold2 | 208427/+1.10%/t=6.4 | 142665/+1.26%/t=4.5 | 161575/+2.18%/t=6.9 | +0.43 pp | 9.6% | `valclose_CJ_gt0` |
| **FK** | last open while FH is on | close | hold2 | 151020/+1.27%/t=6.3 | 100923/+1.70%/t=4.4 | 112401/+2.75%/t=6.5 | +0.87 pp | 8.2% | `valclose_FK_gt0` |
| **FL** | last open while X is on | close | hold2 | 152775/+1.43%/t=6.2 | 102181/+1.68%/t=4.4 | 112948/+3.00%/t=6.7 | +0.85 pp | 9.5% | `valclose_FL_gt0` |
| **IB** | a 0/1 count of HK/HM/HN/HS/HV/IA (IB≥3 was already mined; IB=1 is new) | close | hold2 | 46814/+2.14%/t=4.4 | 31778/+1.48%/t=4.1 | 32349/+3.17%/t=5.0 | +0.64 pp | 8.6% | `flag_IB_eq1` |
| **GV** | a 0/1 composite of several deeper flags | close | hold2 | 71885/+2.23%/t=5.1 | 49429/+2.56%/t=3.5 | 46011/+5.31%/t=5.3 | +1.73 pp | 8.7% | `valclose_GV_eq1` |
| **N** | EL times a 3% same-day move — stored A–O close fill, not past-O | close | hold2 | 57869/+1.30%/t=4.3 | 38944/+1.47%/t=3.4 | 64741/+1.80%/t=4.8 | +0.64 pp | 11.8% | `fill_N_green` |
| **BN** | today's volume vs a ~50-day average | close | hold2 | 61346/+1.14%/t=3.1 | 41897/+2.15%/t=3.3 | 41655/+3.47%/t=4.1 | +1.32 pp | 16.6% | `fill_BN_green` |
| **U** | alias of CV (CV fill was on the old hand list; U value/fill was not) | close | hold2 | 45241/+1.94%/t=4.3 | 30165/+2.39%/t=3.1 | 22399/+6.58%/t=4.9 | +1.56 pp | 10.1% | `fill_U_green` |
| **EJ** | a 0/1 stress flag (AA/AJ/L) | close | hold2 | 101838/+1.48%/t=5.0 | 69618/+1.38%/t=3.1 | 69333/+2.85%/t=4.8 | +0.55 pp | 8.5% | `valclose_EJ_eq1` |
| **FR** | recent volume over 1M and/or G ≥ 3 (open-knowable walk) | open | hold2 | 120379/+0.86%/t=4.3 | 85081/+0.94%/t=3.0 | 71327/+2.42%/t=4.9 | +0.39 pp | 8.9% | `valopen_FR_ge1` |
| **T** | alias of EN | close | hold2 | 40556/+2.19%/t=3.5 | 26772/+3.98%/t=3.0 | 24859/+7.73%/t=4.5 | +3.15 pp | 14.2% | `fill_T_green` |
| **AH** | count of recent same-day drops of 5% or more (open-knowable walk) | open | hold2 | 77814/+1.19%/t=4.0 | 51855/+2.22%/t=2.9 | 42703/+3.89%/t=4.2 | +1.67 pp | 8.7% | `valopen_AH_ge1` |
| **R** | column J when the Change-sheet flag Q is on | close | hold2 | 60746/+1.09%/t=3.0 | 41483/+1.56%/t=2.9 | 42037/+2.87%/t=3.9 | +0.73 pp | 17.0% | `fill_R_green` |
| **IL** | bins same-day return H (close-knowable) | close | hold2 | 39718/+2.86%/t=4.3 | 26155/+3.03%/t=2.9 | 21895/+7.47%/t=4.4 | +2.19 pp | 12.4% | `valclose_IL_eq1` |
| **N** | EL times a 3% same-day move — stored A–O close fill, not past-O | close | hold2 | 37344/+1.74%/t=3.7 | 24856/+1.53%/t=2.8 | 18459/+4.70%/t=4.1 | +0.70 pp | 13.3% | `valclose_N_ge1` |
| **EY** | a short-window max of EX | close | hold2 | 86868/+1.12%/t=5.3 | 58433/+1.28%/t=2.7 | 54568/+2.78%/t=4.6 | +0.45 pp | 12.8% | `valclose_EY_ge1` |
| **AM** | carried-forward deeper state | close | hold2 | 85182/+1.15%/t=4.5 | 56284/+1.36%/t=2.5 | 45810/+3.63%/t=4.5 | +0.53 pp | 8.0% | `valclose_AM_gt0` |
| **HO** | a signed HN/GU cross | close | hold2 | 14286/+2.78%/t=3.6 | 9528/+3.41%/t=2.0 | 8209/+7.36%/t=3.2 | +2.58 pp | 19.8% | `valclose_HO_eq1` |
| **HO** | a signed HN/GU cross | close | hold2 | 14286/+2.78%/t=3.6 | 9528/+3.41%/t=2.0 | 8209/+7.36%/t=3.2 | +2.58 pp | 19.8% | `fill_HO_green` |
| **CZ** | how many recent CP prints were negative | close | hold1 | 194476/+0.69%/t=5.0 | 132153/+0.97%/t=3.5 | 125771/+1.85%/t=5.3 | +0.50 pp | 9.8% | `valclose_CZ_ge2` |
| **FC** | carried-forward FA state | close | hold1 | 174401/+0.79%/t=5.1 | 118554/+1.08%/t=3.5 | 131577/+1.77%/t=5.3 | +0.60 pp | 9.8% | `valclose_FC_ge1` |
| **IK** | carried-forward HO | close | hold1 | 83749/+1.09%/t=4.1 | 55217/+1.33%/t=3.4 | 55908/+2.59%/t=4.9 | +0.86 pp | 10.5% | `valclose_IK_eq1` |
| **BA** | a 0/1 stress flag (HN/CP) | close | hold1 | 112469/+1.09%/t=4.7 | 76076/+1.57%/t=3.4 | 71334/+3.03%/t=5.1 | +1.09 pp | 9.5% | `valclose_BA_eq1` |
| **IB** | a 0/1 count of HK/HM/HN/HS/HV/IA (IB≥3 was already mined; IB=1 is new) | close | hold1 | 47386/+1.22%/t=3.4 | 32167/+1.01%/t=3.3 | 33310/+2.11%/t=4.1 | +0.54 pp | 12.4% | `flag_IB_eq1` |
| **FL** | last open while X is on | close | hold1 | 154328/+0.80%/t=4.6 | 103229/+1.14%/t=3.3 | 115549/+1.94%/t=5.1 | +0.67 pp | 10.5% | `valclose_FL_gt0` |
| **FK** | last open while FH is on | close | hold1 | 152566/+0.72%/t=4.4 | 101965/+1.15%/t=3.3 | 114989/+1.82%/t=4.9 | +0.68 pp | 11.3% | `valclose_FK_gt0` |
| **IL** | bins same-day return H (close-knowable) | close | hold1 | 39986/+1.50%/t=3.7 | 26313/+1.86%/t=3.0 | 22321/+3.99%/t=4.0 | +1.38 pp | 14.7% | `valclose_IL_eq1` |
| **N** | EL times a 3% same-day move — stored A–O close fill, not past-O | close | hold1 | 58866/+0.87%/t=3.1 | 39635/+0.72%/t=2.6 | 66429/+1.10%/t=3.8 | +0.25 pp | 22.6% | `fill_N_green` |
| **GV** | a 0/1 composite of several deeper flags | close | hold1 | 72432/+1.10%/t=3.7 | 49804/+1.78%/t=2.6 | 46933/+3.33%/t=4.0 | +1.30 pp | 11.7% | `valclose_GV_eq1` |
| **N** | EL times a 3% same-day move — stored A–O close fill, not past-O | close | hold1 | 37583/+1.33%/t=3.1 | 25018/+1.32%/t=2.6 | 18860/+3.80%/t=3.6 | +0.84 pp | 23.9% | `valclose_N_ge1` |
| **HZ** | carried-forward HT | close | hold1 | 88317/+0.86%/t=3.7 | 60186/+0.96%/t=2.4 | 65037/+2.00%/t=4.2 | +0.49 pp | 12.5% | `valclose_HZ_eq1` |
| **T** | alias of EN | close | hold1 | 40775/+1.44%/t=3.1 | 26909/+2.99%/t=2.4 | 25215/+5.50%/t=3.6 | +2.52 pp | 12.7% | `fill_T_green` |
| **EY** | a short-window max of EX | close | hold1 | 87368/+0.75%/t=3.6 | 58787/+1.12%/t=2.2 | 55422/+2.26%/t=3.6 | +0.65 pp | 15.8% | `valclose_EY_ge1` |

### Near-miss / top KILL (FAIL, hold1/2 first)

| keep | def | family | clock | side | exit | disc | hold | Q3 | tickers | why |
|---|---|---|---|---|---|---|---|---|---:|---|
| KILL | `fill_DE_red` | fill_new | close | short | hold2 | 40603/-0.04%/t=-0.4 | 27759/+0.11%/t=2.5 | — | 3598 | disc_t,disc_sign,lottery_day,spy_regime,q3_missing |
| KILL | `fill_DE_red` | fill_new | close | short | hold1 | 40603/-0.13%/t=-2.0 | 27759/-0.05%/t=-1.8 | — | 3598 | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_missing |
| KILL | `valclose_BE_ge2` | val_close | close | long | hold1 | 1807/+1.75%/t=10.9 | 1238/+1.71%/t=14.6 | — | 3045 | lottery_day,tape_thin,spy_thin,q3_missing |
| KILL | `valclose_BE_ge1` | val_close | close | long | hold1 | 2013/+1.73%/t=11.6 | 1359/+1.69%/t=14.3 | — | 3372 | lottery_day,tape_thin,spy_thin,q3_missing |
| KILL | `valclose_AO_le-1` | val_close | close | short | hold1 | 1747/+1.62%/t=16.7 | 1202/+1.58%/t=14.3 | — | 2949 | date_bar,lottery_day,tape_thin,spy_thin,q3_missing |

### Ghost that looked like a KEEP (then died)

Column **DE** paints red when the cell equals 0 (green when it equals 1). The formula that writes DE also reads same-day volume and same-day return H, so we only enter at the **close**.
 `fill_DE_red` short hold5: disc 40603/+0.59%/t=4.1, hold 27759/+0.91%/t=14.0, Q3 —, late 327/+2.23%/t=3.0. hold2: 27759/+0.11%/t=2.5. **KILL** (q3_missing).

### Exhaustion

126 leftover cells KEEP on the full Yahoo/rows set. The regions with KEEP 0 are null here; KEEP rows still need another regime before anyone would wire a card.

A–F seed: Yahoo/rows cache via `seed_anchor`. Capture path `lean_rows_cache` · excel STOCKHISTORY cache used: False.

Research only. No cards. Live `flatten_robust` untouched.

## Leftover KEEP harden (unique letters)

# Leftover KEEP harden — unique letters

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge._

## Plain English

The full 3,603-name dump printed **126** leftover KEEP rows. Most of those are the same letter written three ways (equals 1 / at least 1 / greater than 0), or a green highlight and a number that turn on the same days. We keep **one** survivor per letter, side, and hold, prefer the next-day and same-day holds, then re-check the ship bar: Futubull fees, both ticker halves, both calendar halves, **Q3 (2026-07-01)**, both SPY tapes, fattest day under 25% of winning-day P&L, and at least 20 bps better than buying everyone.

Five-session holds that only looked good because the tape was up are dropped. Twin defs are **KILL**. Light+green O on the A–O strip is unchanged. No card.

Raw KEEP **126** → unique letter×side×hold **59** → after ship bar **KEEP 36** · **KILL 23** · **THIN 0**. Sleeve hold1/2 KEEP **36** on letters `AH/AM/BA/BN/CJ/CZ/EH/EJ/EY/FC/FK/FL/FR/GV/HO/HZ/IB/IK/IL/N/R/T/U`. Twins killed **67**. N=3603.

### Shortboard (hold2 survivors)

Ranked by how much they beat 'buy everyone' after fees. Featured letters first (T, BA, AH, CZ, EH, IB), then peers.

| rank | letter | what it is | clock | holdout | vs everyone | Q3 | day-lottery | tickers | verdict |
|---:|---|---|---|---|---|---|---|---:|---|
| 1 | **T** | alias of EN | close | 26772/+3.98%/t=3.0 | +3.15 pp | 24859/+7.73%/t=4.5 | 14.2% | 2827 | **KEEP** |
| 2 | **BA** | a 0/1 stress flag (HN/CP) | close | 75291/+1.96%/t=5.0 | +1.13 pp | 69356/+4.42%/t=7.1 | 10.2% | 3594 | **KEEP** |
| 3 | **AH** | count of recent same-day drops of 5% or more (open-knowable walk) | open | 51855/+2.22%/t=2.9 | +1.67 pp | 42703/+3.89%/t=4.2 | 8.7% | 2802 | **KEEP** |
| 4 | **CZ** | how many recent CP prints were negative | close | 131044/+1.51%/t=5.0 | +0.68 pp | 123062/+2.91%/t=7.0 | 8.7% | 3593 | **KEEP** |
| 5 | **EH** | any of FP–FU is negative → 4, else 0 | close | 124724/+1.34%/t=5.4 | +0.51 pp | 116468/+2.74%/t=7.3 | 9.4% | 3598 | **KEEP** |
| 6 | **IB** | a 0/1 count of HK/HM/HN/HS/HV/IA (IB≥3 was already mined; IB=1 is new) | close | 31778/+1.48%/t=4.1 | +0.64 pp | 32349/+3.17%/t=5.0 | 8.6% | 3595 | **KEEP** |
| 7 | **HO** | a signed HN/GU cross | close | 9528/+3.41%/t=2.0 | +2.58 pp | 8209/+7.36%/t=3.2 | 19.8% | 2961 | **KEEP** |
| 8 | **IL** | bins same-day return H (close-knowable) | close | 26155/+3.03%/t=2.9 | +2.19 pp | 21895/+7.47%/t=4.4 | 12.4% | 3315 | **KEEP** |
| 9 | **GV** | a 0/1 composite of several deeper flags | close | 49429/+2.56%/t=3.5 | +1.73 pp | 46011/+5.31%/t=5.3 | 8.7% | 3598 | **KEEP** |
| 10 | **U** | alias of CV (CV fill was on the old hand list; U value/fill was not) | close | 30165/+2.39%/t=3.1 | +1.56 pp | 22399/+6.58%/t=4.9 | 10.1% | 3025 | **KEEP** |
| 11 | **IK** | carried-forward HO | close | 54754/+2.16%/t=4.6 | +1.33 pp | 54787/+3.86%/t=6.2 | 8.4% | 2961 | **KEEP** |
| 12 | **FK** | last open while FH is on | close | 100923/+1.70%/t=4.4 | +0.87 pp | 112401/+2.75%/t=6.5 | 8.2% | 2587 | **KEEP** |
| 13 | **FL** | last open while X is on | close | 102181/+1.68%/t=4.4 | +0.85 pp | 112948/+3.00%/t=6.7 | 9.5% | 3041 | **KEEP** |
| 14 | **FC** | carried-forward FA state | close | 117363/+1.65%/t=4.8 | +0.82 pp | 128649/+2.77%/t=7.0 | 9.0% | 2928 | **KEEP** |
| 15 | **R** | column J when the Change-sheet flag Q is on | close | 41483/+1.56%/t=2.9 | +0.73 pp | 42037/+2.87%/t=3.9 | 17.0% | 3588 | **KEEP** |
| 16 | **N** | EL times a 3% same-day move — stored A–O close fill, not past-O | close | 38944/+1.47%/t=3.4 | +0.64 pp | 64741/+1.80%/t=4.8 | 11.8% | 3594 | **KEEP** |
| 17 | **BN** | today's volume vs a ~50-day average | close | 129756/+1.39%/t=4.5 | +0.56 pp | 160019/+2.21%/t=6.9 | 10.0% | 3589 | **KEEP** |
| 18 | **EJ** | a 0/1 stress flag (AA/AJ/L) | close | 69618/+1.38%/t=3.1 | +0.55 pp | 69333/+2.85%/t=4.8 | 8.5% | 3579 | **KEEP** |
| 19 | **AM** | carried-forward deeper state | close | 56284/+1.36%/t=2.5 | +0.53 pp | 45810/+3.63%/t=4.5 | 8.0% | 3057 | **KEEP** |
| 20 | **EY** | a short-window max of EX | close | 58433/+1.28%/t=2.7 | +0.45 pp | 54568/+2.78%/t=4.6 | 12.8% | 2986 | **KEEP** |
| 21 | **CJ** | a running price sum / 50 | close | 142665/+1.26%/t=4.5 | +0.43 pp | 161575/+2.18%/t=6.9 | 9.6% | 3598 | **KEEP** |
| 22 | **FR** | recent volume over 1M and/or G ≥ 3 (open-knowable walk) | open | 85081/+0.94%/t=3.0 | +0.39 pp | 71327/+2.42%/t=4.9 | 8.9% | 3472 | **KEEP** |
| 23 | **HZ** | carried-forward HT | close | 151661/+1.20%/t=4.6 | +0.37 pp | 159892/+2.20%/t=6.9 | 8.9% | 3573 | **KEEP** |

### Unique-letter KEEP list (hold1/2)

| letter | hold | clock | side | what it is | disc | holdout | Q3 | vs everyone | day-lottery | survivor def |
|---|---|---|---|---|---|---|---|---|---|---|
| **T** | hold2 | close | long | alias of EN | 40556/+2.19%/t=3.5 | 26772/+3.98%/t=3.0 | 24859/+7.73%/t=4.5 | +3.15 pp | 14.2% | `fill_T_green` |
| **HO** | hold2 | close | long | a signed HN/GU cross | 14286/+2.78%/t=3.6 | 9528/+3.41%/t=2.0 | 8209/+7.36%/t=3.2 | +2.58 pp | 19.8% | `valclose_HO_eq1` |
| **IL** | hold2 | close | long | bins same-day return H (close-knowable) | 39718/+2.86%/t=4.3 | 26155/+3.03%/t=2.9 | 21895/+7.47%/t=4.4 | +2.19 pp | 12.4% | `valclose_IL_eq1` |
| **GV** | hold2 | close | long | a 0/1 composite of several deeper flags | 71885/+2.23%/t=5.1 | 49429/+2.56%/t=3.5 | 46011/+5.31%/t=5.3 | +1.73 pp | 8.7% | `valclose_GV_eq1` |
| **AH** | hold2 | open | long | count of recent same-day drops of 5% or more (open-knowable walk) | 77814/+1.19%/t=4.0 | 51855/+2.22%/t=2.9 | 42703/+3.89%/t=4.2 | +1.67 pp | 8.7% | `valopen_AH_ge1` |
| **U** | hold2 | close | long | alias of CV (CV fill was on the old hand list; U value/fill was not) | 45241/+1.94%/t=4.3 | 30165/+2.39%/t=3.1 | 22399/+6.58%/t=4.9 | +1.56 pp | 10.1% | `fill_U_green` |
| **IK** | hold2 | close | long | carried-forward HO | 83091/+1.86%/t=5.7 | 54754/+2.16%/t=4.6 | 54787/+3.86%/t=6.2 | +1.33 pp | 8.4% | `valclose_IK_eq1` |
| **BA** | hold2 | close | long | a 0/1 stress flag (HN/CP) | 111276/+2.06%/t=6.5 | 75291/+1.96%/t=5.0 | 69356/+4.42%/t=7.1 | +1.13 pp | 10.2% | `valclose_BA_eq1` |
| **FK** | hold2 | close | long | last open while FH is on | 151020/+1.27%/t=6.3 | 100923/+1.70%/t=4.4 | 112401/+2.75%/t=6.5 | +0.87 pp | 8.2% | `valclose_FK_gt0` |
| **FL** | hold2 | close | long | last open while X is on | 152775/+1.43%/t=6.2 | 102181/+1.68%/t=4.4 | 112948/+3.00%/t=6.7 | +0.85 pp | 9.5% | `valclose_FL_gt0` |
| **FC** | hold2 | close | long | carried-forward FA state | 172664/+1.44%/t=6.9 | 117363/+1.65%/t=4.8 | 128649/+2.77%/t=7.0 | +0.82 pp | 9.0% | `valclose_FC_ge1` |
| **R** | hold2 | close | long | column J when the Change-sheet flag Q is on | 60746/+1.09%/t=3.0 | 41483/+1.56%/t=2.9 | 42037/+2.87%/t=3.9 | +0.73 pp | 17.0% | `fill_R_green` |
| **CZ** | hold2 | close | long | how many recent CP prints were negative | 192876/+1.30%/t=7.0 | 131044/+1.51%/t=5.0 | 123062/+2.91%/t=7.0 | +0.68 pp | 8.7% | `valclose_CZ_ge2` |
| **IB** | hold2 | close | long | a 0/1 count of HK/HM/HN/HS/HV/IA (IB≥3 was already mined; IB=1 is new) | 46814/+2.14%/t=4.4 | 31778/+1.48%/t=4.1 | 32349/+3.17%/t=5.0 | +0.64 pp | 8.6% | `flag_IB_eq1` |
| **N** | hold2 | close | long | EL times a 3% same-day move — stored A–O close fill, not past-O | 57869/+1.30%/t=4.3 | 38944/+1.47%/t=3.4 | 64741/+1.80%/t=4.8 | +0.64 pp | 11.8% | `fill_N_green` |
| **BN** | hold2 | close | long | today's volume vs a ~50-day average | 189800/+1.19%/t=6.3 | 129756/+1.39%/t=4.5 | 160019/+2.21%/t=6.9 | +0.56 pp | 10.0% | `valclose_BN_gt0` |
| **EJ** | hold2 | close | long | a 0/1 stress flag (AA/AJ/L) | 101838/+1.48%/t=5.0 | 69618/+1.38%/t=3.1 | 69333/+2.85%/t=4.8 | +0.55 pp | 8.5% | `valclose_EJ_eq1` |
| **AM** | hold2 | close | long | carried-forward deeper state | 85182/+1.15%/t=4.5 | 56284/+1.36%/t=2.5 | 45810/+3.63%/t=4.5 | +0.53 pp | 8.0% | `valclose_AM_gt0` |
| **EH** | hold2 | close | long | any of FP–FU is negative → 4, else 0 | 180453/+1.31%/t=6.7 | 124724/+1.34%/t=5.4 | 116468/+2.74%/t=7.3 | +0.51 pp | 9.4% | `valclose_EH_ge1` |
| **EY** | hold2 | close | long | a short-window max of EX | 86868/+1.12%/t=5.3 | 58433/+1.28%/t=2.7 | 54568/+2.78%/t=4.6 | +0.45 pp | 12.8% | `valclose_EY_ge1` |
| **CJ** | hold2 | close | long | a running price sum / 50 | 208427/+1.10%/t=6.4 | 142665/+1.26%/t=4.5 | 161575/+2.18%/t=6.9 | +0.43 pp | 9.6% | `valclose_CJ_gt0` |
| **FR** | hold2 | open | long | recent volume over 1M and/or G ≥ 3 (open-knowable walk) | 120379/+0.86%/t=4.3 | 85081/+0.94%/t=3.0 | 71327/+2.42%/t=4.9 | +0.39 pp | 8.9% | `valopen_FR_ge1` |
| **HZ** | hold2 | close | long | carried-forward HT | 221745/+1.09%/t=6.7 | 151661/+1.20%/t=4.6 | 159892/+2.20%/t=6.9 | +0.37 pp | 8.9% | `valclose_HZ_ge1` |
| **T** | hold1 | close | long | alias of EN | 40775/+1.44%/t=3.1 | 26909/+2.99%/t=2.4 | 25215/+5.50%/t=3.6 | +2.52 pp | 12.7% | `fill_T_green` |
| **IL** | hold1 | close | long | bins same-day return H (close-knowable) | 39986/+1.50%/t=3.7 | 26313/+1.86%/t=3.0 | 22321/+3.99%/t=4.0 | +1.38 pp | 14.7% | `valclose_IL_eq1` |
| **GV** | hold1 | close | long | a 0/1 composite of several deeper flags | 72432/+1.10%/t=3.7 | 49804/+1.78%/t=2.6 | 46933/+3.33%/t=4.0 | +1.30 pp | 11.7% | `valclose_GV_eq1` |
| **BA** | hold1 | close | long | a 0/1 stress flag (HN/CP) | 112469/+1.09%/t=4.7 | 76076/+1.57%/t=3.4 | 71334/+3.03%/t=5.1 | +1.09 pp | 9.5% | `valclose_BA_eq1` |
| **IK** | hold1 | close | long | carried-forward HO | 83749/+1.09%/t=4.1 | 55217/+1.33%/t=3.4 | 55908/+2.59%/t=4.9 | +0.86 pp | 10.5% | `valclose_IK_eq1` |
| **FK** | hold1 | close | long | last open while FH is on | 152566/+0.72%/t=4.4 | 101965/+1.15%/t=3.3 | 114989/+1.82%/t=4.9 | +0.68 pp | 11.3% | `valclose_FK_gt0` |
| **FL** | hold1 | close | long | last open while X is on | 154328/+0.80%/t=4.6 | 103229/+1.14%/t=3.3 | 115549/+1.94%/t=5.1 | +0.67 pp | 10.5% | `valclose_FL_gt0` |
| **EY** | hold1 | close | long | a short-window max of EX | 87368/+0.75%/t=3.6 | 58787/+1.12%/t=2.2 | 55422/+2.26%/t=3.6 | +0.65 pp | 15.8% | `valclose_EY_ge1` |
| **FC** | hold1 | close | long | carried-forward FA state | 174401/+0.79%/t=5.1 | 118554/+1.08%/t=3.5 | 131577/+1.77%/t=5.3 | +0.60 pp | 9.8% | `valclose_FC_ge1` |
| **IB** | hold1 | close | long | a 0/1 count of HK/HM/HN/HS/HV/IA (IB≥3 was already mined; IB=1 is new) | 47386/+1.22%/t=3.4 | 32167/+1.01%/t=3.3 | 33310/+2.11%/t=4.1 | +0.54 pp | 12.4% | `flag_IB_eq1` |
| **CZ** | hold1 | close | long | how many recent CP prints were negative | 194476/+0.69%/t=5.0 | 132153/+0.97%/t=3.5 | 125771/+1.85%/t=5.3 | +0.50 pp | 9.8% | `valclose_CZ_ge2` |
| **HZ** | hold1 | close | long | carried-forward HT | 88317/+0.86%/t=3.7 | 60186/+0.96%/t=2.4 | 65037/+2.00%/t=4.2 | +0.49 pp | 12.5% | `valclose_HZ_eq1` |
| **N** | hold1 | close | long | EL times a 3% same-day move — stored A–O close fill, not past-O | 58866/+0.87%/t=3.1 | 39635/+0.72%/t=2.6 | 66429/+1.10%/t=3.8 | +0.25 pp | 22.6% | `fill_N_green` |

### Killed twins

Same letter, same days (or a weaker fill/value of the same letter). Not a second edge.

| killed def | letter | hold | why | survivor |
|---|---|---|---|---|
| `valopen_AH_ge2` | AH | hold2 | twin_weaker | `valopen_AH_ge1` |
| `valopen_AH_gt0` | AH | hold2 | twin_same_days | `valopen_AH_ge1` |
| `valopen_AH_ge2` | AH | hold5 | twin_weaker | `valopen_AH_ge1` |
| `valopen_AH_gt0` | AH | hold5 | twin_same_days | `valopen_AH_ge1` |
| `valclose_BA_ge1` | BA | hold1 | twin_same_days | `valclose_BA_eq1` |
| `valclose_BA_gt0` | BA | hold1 | twin_same_days | `valclose_BA_eq1` |
| `valclose_BA_ge1` | BA | hold2 | twin_same_days | `valclose_BA_eq1` |
| `valclose_BA_gt0` | BA | hold2 | twin_same_days | `valclose_BA_eq1` |
| `valclose_BA_ge1` | BA | hold5 | twin_same_days | `valclose_BA_eq1` |
| `valclose_BA_gt0` | BA | hold5 | twin_same_days | `valclose_BA_eq1` |
| `fill_BN_green` | BN | hold2 | twin_weaker | `valclose_BN_gt0` |
| `valclose_BN_ge1` | BN | hold2 | twin_weaker | `valclose_BN_gt0` |
| `fill_BN_green` | BN | hold5 | twin_weaker | `valclose_BN_gt0` |
| `valclose_BN_ge1` | BN | hold5 | twin_weaker | `valclose_BN_gt0` |
| `valclose_CZ_ge1` | CZ | hold2 | twin_weaker | `valclose_CZ_ge2` |
| `valclose_CZ_gt0` | CZ | hold2 | twin_weaker | `valclose_CZ_ge2` |
| `valclose_CZ_ge1` | CZ | hold5 | twin_weaker | `valclose_CZ_ge2` |
| `valclose_CZ_gt0` | CZ | hold5 | twin_weaker | `valclose_CZ_ge2` |
| `valclose_EH_ge2` | EH | hold2 | twin_same_days | `valclose_EH_ge1` |
| `valclose_EH_gt0` | EH | hold2 | twin_same_days | `valclose_EH_ge1` |
| `valclose_EH_ge2` | EH | hold5 | twin_same_days | `valclose_EH_ge1` |
| `valclose_EH_gt0` | EH | hold5 | twin_same_days | `valclose_EH_ge1` |
| `valclose_EJ_ge1` | EJ | hold2 | twin_same_days | `valclose_EJ_eq1` |
| `valclose_EJ_gt0` | EJ | hold2 | twin_same_days | `valclose_EJ_eq1` |
| `valclose_EJ_ge1` | EJ | hold5 | twin_same_days | `valclose_EJ_eq1` |
| `valclose_EJ_gt0` | EJ | hold5 | twin_same_days | `valclose_EJ_eq1` |
| `valclose_EY_gt0` | EY | hold1 | twin_same_days | `valclose_EY_ge1` |
| `valclose_EY_eq1` | EY | hold2 | twin_weaker | `valclose_EY_ge1` |
| `valclose_EY_gt0` | EY | hold2 | twin_same_days | `valclose_EY_ge1` |
| `valclose_EY_eq1` | EY | hold5 | twin_weaker | `valclose_EY_ge1` |
| `valclose_EY_gt0` | EY | hold5 | twin_same_days | `valclose_EY_ge1` |
| `valclose_FC_eq1` | FC | hold1 | twin_weaker | `valclose_FC_ge1` |
| `valclose_FC_gt0` | FC | hold1 | twin_same_days | `valclose_FC_ge1` |
| `valclose_FC_eq1` | FC | hold2 | twin_weaker | `valclose_FC_ge1` |
| `valclose_FC_gt0` | FC | hold2 | twin_same_days | `valclose_FC_ge1` |
| `valclose_FC_eq1` | FC | hold5 | twin_weaker | `valclose_FC_ge1` |
| `valclose_FC_gt0` | FC | hold5 | twin_same_days | `valclose_FC_ge1` |
| `valopen_FR_gt0` | FR | hold2 | twin_same_days | `valopen_FR_ge1` |
| `valopen_FR_gt0` | FR | hold5 | twin_same_days | `valopen_FR_ge1` |
| `valclose_GV_ge1` | GV | hold1 | twin_same_days | `valclose_GV_eq1` |
| `valclose_GV_gt0` | GV | hold1 | twin_same_days | `valclose_GV_eq1` |
| `valclose_GV_ge1` | GV | hold2 | twin_same_days | `valclose_GV_eq1` |
| `valclose_GV_gt0` | GV | hold2 | twin_same_days | `valclose_GV_eq1` |
| `valclose_GV_ge1` | GV | hold5 | twin_same_days | `valclose_GV_eq1` |
| `valclose_GV_gt0` | GV | hold5 | twin_same_days | `valclose_GV_eq1` |
| `fill_HO_green` | HO | hold2 | twin_same_days | `valclose_HO_eq1` |
| `valclose_HO_ge1` | HO | hold2 | twin_same_days | `valclose_HO_eq1` |
| `valclose_HO_gt0` | HO | hold2 | twin_same_days | `valclose_HO_eq1` |
| `fill_HO_green` | HO | hold5 | twin_same_days | `valclose_HO_eq1` |
| `valclose_HO_ge1` | HO | hold5 | twin_same_days | `valclose_HO_eq1` |
| `valclose_HO_gt0` | HO | hold5 | twin_same_days | `valclose_HO_eq1` |
| `valclose_HZ_eq1` | HZ | hold2 | twin_weaker | `valclose_HZ_ge1` |
| `valclose_HZ_gt0` | HZ | hold2 | twin_same_days | `valclose_HZ_ge1` |
| `valclose_HZ_eq1` | HZ | hold5 | twin_weaker | `valclose_HZ_ge1` |
| `valclose_HZ_gt0` | HZ | hold5 | twin_same_days | `valclose_HZ_ge1` |
| `valclose_IK_ge1` | IK | hold1 | twin_same_days | `valclose_IK_eq1` |
| `valclose_IK_gt0` | IK | hold1 | twin_same_days | `valclose_IK_eq1` |
| `valclose_IK_ge1` | IK | hold2 | twin_same_days | `valclose_IK_eq1` |
| `valclose_IK_gt0` | IK | hold2 | twin_same_days | `valclose_IK_eq1` |
| `valclose_IK_ge1` | IK | hold5 | twin_same_days | `valclose_IK_eq1` |
| `valclose_IK_gt0` | IK | hold5 | twin_same_days | `valclose_IK_eq1` |
| `valclose_N_ge1` | N | hold1 | twin_weaker | `fill_N_green` |
| `valclose_N_gt0` | N | hold1 | twin_weaker | `fill_N_green` |
| `valclose_N_ge1` | N | hold2 | twin_weaker | `fill_N_green` |
| `valclose_N_gt0` | N | hold2 | twin_weaker | `fill_N_green` |
| `valclose_N_ge1` | N | hold5 | twin_weaker | `fill_N_green` |
| `valclose_N_gt0` | N | hold5 | twin_weaker | `fill_N_green` |

### Survivors that died on re-judge

| letter | hold | def | verdict | why |
|---|---|---|---|---|
| HO | hold5 | `valclose_HO_eq1` | **KILL** | hold5_not_primary |
| T | hold5 | `fill_T_green` | **KILL** | hold5_not_primary |
| U | hold5 | `fill_U_green` | **KILL** | hold5_not_primary |
| IK | hold5 | `valclose_IK_eq1` | **KILL** | hold5_not_primary |
| GV | hold5 | `valclose_GV_eq1` | **KILL** | hold5_not_primary |
| BA | hold5 | `valclose_BA_eq1` | **KILL** | hold5_not_primary |
| AH | hold5 | `valopen_AH_ge1` | **KILL** | hold5_not_primary |
| IL | hold5 | `valclose_IL_eq1` | **KILL** | hold5_not_primary |
| N | hold5 | `fill_N_green` | **KILL** | hold5_not_primary |
| IB | hold5 | `flag_IB_eq1` | **KILL** | hold5_not_primary |
| FC | hold5 | `valclose_FC_ge1` | **KILL** | hold5_not_primary |
| FK | hold5 | `valclose_FK_gt0` | **KILL** | hold5_not_primary |
| EH | hold5 | `valclose_EH_ge1` | **KILL** | hold5_not_primary |
| CZ | hold5 | `valclose_CZ_ge2` | **KILL** | hold5_not_primary |
| EY | hold5 | `valclose_EY_ge1` | **KILL** | hold5_not_primary |
| FL | hold5 | `valclose_FL_gt0` | **KILL** | hold5_not_primary |
| FR | hold5 | `valopen_FR_ge1` | **KILL** | hold5_not_primary |
| R | hold5 | `fill_R_green` | **KILL** | hold5_not_primary |
| BN | hold5 | `valclose_BN_gt0` | **KILL** | hold5_not_primary |
| AM | hold5 | `valclose_AM_gt0` | **KILL** | hold5_not_primary |
| EJ | hold5 | `valclose_EJ_eq1` | **KILL** | hold5_not_primary |
| CJ | hold5 | `valclose_CJ_gt0` | **KILL** | hold5_not_primary |
| HZ | hold5 | `valclose_HZ_ge1` | **KILL** | hold5_not_primary |

### What this does not change

- Standing A–O research keeps stay the **three light + green O** recipes. This table does not touch them.
- Finviz volume stays **BLOCKED**. AB / weather / book stay dead.
- Live `flatten_robust` is not imported or changed. No cards.
- Calendar half cut **2026-05-01**. Q3 cut **2026-07-01**. Futubull 0.15% long / 0.20% short.

Research only. One 2026 regime.
