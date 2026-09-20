# How we find overnight bangers — kid-plain report

_Research only. Generated 2026-09-19. Live `flatten_robust` and Webull stay sit._

## What we were trying to own

A fat up-day is often **already printed before 09:30**. Thursday 09-17 SPX +1.14% close-to-close was +1.05% in the overnight gap and only +0.08% from the open. A 09:30 long that buys *that* morning misses the bang.

To own the bang you must buy **yesterday at 09:30** and still be holding at **today's open**.

## What the repo already prints (and what we used)

| Resource | Knowable at 09:30? | Did it help find the bang? |
|---|---|---|
| Prior Finviz **Earnings Date** (AMC today / BMO next) | Yes — yesterday's export | **Yes, this is the print-night list** |
| Morning cameras (news, vol, catal, 🚨 alarm, white) | Yes — pre-open packet | **🚨 off + $50B is the useful slice** |
| Prior tape (last green, 5-day heat, mcap) | Yes — bars with date < D | **$50B filter is the one-way cut** |
| Yesterday's liquid winners | Yes — prior Change% | **Yes, for index-like nights** |
| Morning weather S | Yes — predict file | Holdup uses S>0. Hard-red (S≤−3) still sits, including the 09-02 print night |
| Excel time-split factor mine | Open-knowable letters | **No.** KEEP = 0. Best prove after-fee WR 54.9% ≤ 55% |
| Excel-bot daily longs (L1/L2/L3) | Signal colors on the sheet | **No.** Live means −3% to −5% |

Same-day Change%, Gap, and RelVol are **never** inputs.

## Two different nights, two lists

### 1. Print night — the calendar

At D 09:30, take names yesterday's Finviz export already listed as:

- **AMC today** (time ≥ 16:00 — the print has not happened), or
- **BMO next session** (tomorrow morning, still ahead)

Buy them at D 09:30. Hold 1 sells at the next 09:30, so D-close → D+1-open is in the book.

`earn_react` / `e_fresh` fire **after** the print. They miss this night on purpose.

Date-only "earnings today" stays off the list — that could already have been BMO.

### 2. Index-like night — yesterday's winners

Thursday 09-17 was this kind of night. The scheduled leftover was ALMU/LEN (−6.58% next open), not mega-cap beta. Yesterday's liquid 25 gapped **+1.30%**. That is `yday_gainer_h1` / `union_hot_n4_holdup` when morning S > 0.

## What the cameras and size cut actually did

Equal-weight **D close → D+1 open**, Aug 14 → last closed, 191 priced calendar name-nights:

| Slice | n | EW gap | +5% bangs | −5% dumps |
|---|---:|---:|---:|---:|
| All liquid scheduled | 191 | **−0.61%** | 21% | 30% |
| Mega-cap ($50B+) | 31 | **+1.42%** | 26% | 13% |
| Mega ∩ no 🚨 alarm | 24 | **+2.17%** | 29% | 12% |
| Mega ∩ last bar green | 17 | +0.11% | 12% | 12% |
| News-packet green | 5 | +0.24% | too thin | |
| Catal green | 0 | — | cameras almost never light before the print | |
| Yesterday's liquid 25 | — | **+1.75%** | index-like nights | |

The unfiltered calendar is a coin flip (more dumps than bangs). **$50B + no overnight alarm** is the camera/size cut that turns it one-way on the *gap*. News and catalyst cameras are usually still blank — the print has not happened yet, so those boxes cannot see it.

## Cash-book result (09:30 fill, Futubull fees, hard-red sit)

Buying at D 09:30 also eats **that day's open→close** before the print. Hard-red sit still blocks new buys (09-02, the fattest print night in this window, was S=−3.83).

| Sleeve | Cash Book% | Trades | What it is |
|---|---:|---:|---|
| `union_hot_n4_holdup` | **+56.52%** | 74 | S>0 leftover heat, kept through the next open |
| `union_hot_n4_h1` | +31.73% | 94 | same names, hold 1, **misses** the next gap |
| `combo_oh_5050_shared` | +18.61% | 114 | mega calendar + holdup sharing one $10k — **worse than holdup alone** |
| `overnight_mega_h2` | +0.50% | 6 | too thin |
| `overnight_mega_h1` | −0.27% | 20 | mega ∩ no 🚨, hold 1 |
| `overnight_mega_green_h1` | −3.38% | 16 | extra last-green cut hurt |
| `overnight_h1` | −19.04% | 98 | unfiltered calendar — do not use |

So: the calendar **finds** print-night names, and mega+no-alarm **cleans the gap**. It does **not** beat leftover holdup as a cash book, because we still buy a full session before the print and we still sit hard-red mornings.

## How this is better than what we had last week

1. **We can say out loud who to buy for a print night** — yesterday's earnings calendar, $50B+, no 🚨 — instead of hoping leftover hot4 is "the market."
2. **We can say out loud who to hold for an index-like night** — yesterday's liquid winners, keep the lot through the next 09:30 when S>0 (`union_hot_n4_holdup`). That is the published Book% lift: **+56.52% vs +31.73%** on the same heat names.
3. **We know what does not work** — buy-all scheduled names (−19% book), Excel KEEP (0 rules), Excel-bot color longs (live mean red), mixing mega calendar 50/50 with holdup (dilutes to +18.6%).

## What we still do not do

- We do not change live `flatten_robust`.
- We do not turn off hard-red sit.
- We do not peek at today's Change% to pick the list.
- We do not call overnight_mega a live wire. The gap slice is real; the cash book is not richer than holdup.
