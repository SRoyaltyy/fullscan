# HOT4 paper orders vs the rebuilt book

Window `2026-09-22` through `2026-09-25`. Three lists, kept apart:

- Webull paper orders the broker acknowledged, from `03_scoreboard/factor_mine/hot4_webull_orders_2026-09-22_2026-09-25.csv`. Fill prices are the broker's final fills from the paper record (SEC/FINRA fees; commission 0). The 09-25 rows were still working at 08:14 ET, unfilled.
- The ticket file at send time. Commits: `18039b02cf` (09-22), `409c73e31a` (09-23), `36416dd8a9` (09-24), `9498138de8` (09-25). Each commit subject is `auto: strategy tickets` for that date.
- The rebuilt sequential book `union_hot_n4_h1` in `data/factor_mine/state/union_hot_n4_h1/<date>.json`, also listed in `03_scoreboard/factor_mine/union_hot_n4_h1_orders_2026-09-22_2026-09-25.csv`. That book is the $10,000 Futubull sleeve. Webull sized a different cash balance, so share counts are a different account.

09-22 paper orders are the old wire. PR #319 merged at 2026-09-22 23:53 ET, so 09-23 is the first session on the cash-start wire.

The dated ticket files in the tree today are later overwrites. They are listed last so they are not used as the send-time list. After this change, `strategy_tickets.write` leaves `data/day_board/<date>_strategy_tickets.json` in place once that session's 09:30 ET has arrived.

## 2026-09-22

Send-time commit `18039b02cf` at 2026-09-22 08:30 ET. HOT4 status `ok`, S `-0.497`. Tickets: BUY AMD, ABVX, MRAM, GME. No sells.

| | side | ticker | shares | price |
|---|---|---|---:|---:|
| Webull | BUY | AMD | 131 | 607.13 |
| Webull | BUY | ABVX | 772 | 102.89 |
| Webull | BUY | MRAM | 4759 | 17.03 |
| Rebuilt | SELL | CYPH | 629 | 3.51 |
| Rebuilt | SELL | FEAM | 1019 | 2.47 |
| Rebuilt | SELL | LVWR | 1746 | 1.5 |
| Rebuilt | SELL | TJGC | 147 | 17.58 |
| Rebuilt | BUY | GRAL | 23 | 106.75 |
| Rebuilt | BUY | NUAI | 342 | 7.23 |
| Rebuilt | BUY | ARM | 7 | 319.41 |
| Rebuilt | BUY | INDP | 798 | 3.1 |

Webull sent AMD, ABVX, and MRAM. GME was on the ticket and was not sent. The paper record says that name was already held from 09-21. No Webull ticker is in the rebuilt list. Rebuilt session return on the $10k book is 7.057% (equity $10,637.65).

The dated file in the tree is the send-time body from `18039b02cf` (BUY AMD, ABVX, MRAM, GME). The evening overwrite was restored and logged in `data/day_board/RESTATEMENTS.log`.

## 2026-09-23

Send-time commit `409c73e31a` at 2026-09-23 06:29 ET. HOT4 status `ok`, S `2.293`. Tickets: BUY INDP, XHLD, GPRO, INSP. SELL SECZ, GRAL, NUAI.

| | side | ticker | shares | price |
|---|---|---|---:|---:|
| Webull | BUY | INDP | 557 | 3.91 |
| Webull | BUY | XHLD | 168 | 11.51 |
| Webull | BUY | GPRO | 1365 | 1.28 |
| Webull | BUY | INSP | 23 | 71.77 |
| Rebuilt | SELL | ARM | 7 | 331.78 |
| Rebuilt | SELL | GRAL | 23 | 108.52 |
| Rebuilt | SELL | INDP | 798 | 3.93 |
| Rebuilt | SELL | NUAI | 342 | 6.83 |
| Rebuilt | BUY | FEAM | 899 | 2.92 |
| Rebuilt | BUY | GLND | 973 | 2.7 |
| Rebuilt | BUY | VICR | 9 | 266.5 |
| Rebuilt | BUY | VKTX | 62 | 41.76 |

Webull bought the four ticket names and did not sell. The paper record says SECZ, GRAL, and NUAI were never held, so those sells were skipped. The rebuilt book sold GRAL, INDP, and NUAI and bought FEAM, GLND, VICR, and VKTX. The shared name is INDP, on opposite sides: Webull bought 557 at 3.91, the rebuild sold 798 at 3.93. Rebuilt session return is -0.5555% (equity $10,578.56).

The dated file in the tree is the send-time body from `409c73e31a` (BUY INDP, XHLD, GPRO, INSP; SELL SECZ, GRAL, NUAI). The evening overwrite was restored and logged in `data/day_board/RESTATEMENTS.log`.

## 2026-09-24

Send-time commit `36416dd8a9` at 2026-09-24 06:19 ET. HOT4 status `sit`, S `-7.659`, hard-red. Tickets: BUY GPRO, INSP, TJGC, AIB. SELL GLND, FEAM, XHLD, INDP.

| | side | ticker | shares | price |
|---|---|---|---:|---:|
| Webull | SELL | XHLD | 168 | 12.08 |
| Webull | SELL | INDP | 557 | 3.71 |
| Rebuilt | SELL | FEAM | 899 | 2.61 |
| Rebuilt | SELL | VKTX | 62 | 36.025 |

Webull sold XHLD and INDP, the two ticket sells it actually held. It bought nothing. The paper record says the hard-red sit blocked new longs, and GLND and FEAM were never held. The rebuilt book sold FEAM and VKTX and bought nothing. FEAM is on the ticket sell list and on the rebuild; Webull did not sell it. XHLD and INDP were sold at Webull and are absent from the rebuild that day (the rebuild had sold INDP the session before). Rebuilt session return is 18.1549% (equity $12,499.09). That equity jump is the $10k book's mark, including GLND still held, and is a different account from the paper P&L.

The dated file in the tree is the send-time body from `36416dd8a9` (sit, BUY GPRO, INSP, TJGC, AIB; SELL GLND, FEAM, XHLD, INDP). The evening overwrite was restored and logged in `data/day_board/RESTATEMENTS.log`.

## 2026-09-25

Send-time commit `9498138de8` at 2026-09-25 07:20 ET. HOT4 status `ok`, S `2.706`. Tickets: BUY GPRO, INSP, TJGC, QRVO. SELL GLND.

| | side | ticker | shares | price |
|---|---|---|---:|---:|
| Webull | BUY | TJGC | 82 | working at 08:14 ET |
| Webull | BUY | QRVO | 15 | working at 08:14 ET |
| Rebuilt | | | | not locked |

Webull sent TJGC and QRVO. GPRO and INSP were already held from 09-23, and GLND was never held, so those ticket rows were not sent. The paper record says one send, run 36128068845 at 07:20 ET, and two later runs logged that the session was already attempted. The rebuilt book has no 2026-09-25 snapshot, so that day is `not_locked` and has no orders.

There is no `data/day_board/2026-09-25_strategy_tickets.json` in this tree.

## Input trace

One row per day. The send-time ticket is the commit named below. The mine commit is the last `03_scoreboard/factor_mine.json` on that commit's parent, which is the tree the ticket job had checked out. The frozen snapshot is `data/factor_mine/snapshots/<date>.json`. Its `sources` pins are the last commit of each morning file before 09:30 ET.

| date | send | mine the writer had | input that moved the list |
| --- | --- | --- | --- |
| 2026-09-22 | `18039b02cf` 08:30 ET. BUY AMD, ABVX, MRAM, GME. | `087d5fda40` 2026-09-21 17:51 ET. Panel ends 2026-09-21. | The 12 pinned morning files match the send parent byte for byte. The panel had no 09-22 rows, so the ticket used the live session look. AMD and GME are liquid_tape candidates and are not among the 72 frozen rows. The rebuild bought GRAL, NUAI, ARM, INDP from those rows. Snapshot code `693b0b078bba` is not in the send parent. |
| 2026-09-23 | `409c73e31a` 06:29 ET. BUY INDP, XHLD, GPRO, INSP. SELL SECZ, GRAL, NUAI. | `41bcc47fe1` 2026-09-22 20:04 ET. Panel ends 2026-09-22. | The 12 pinned morning files match the send parent. GPRO and INSP are not frozen rows. SECZ is dropped: indicator bars 58/60. The rebuild bought FEAM, GLND, VICR, VKTX, which are frozen rows. |
| 2026-09-24 | `36416dd8a9` 06:19 ET. Sit, S -7.659. BUY GPRO, INSP, TJGC, AIB. SELL GLND, FEAM, XHLD, INDP. | `0d3fdf1110` 2026-09-23 17:16 ET. Panel ends 2026-09-23. | Six snapshot pins are not the send parent's bytes. Missing at send: `01_daily/catalyst/2026-09-24_dossiers.json` and `01_daily/map_heat/2026-09-24_research.json`. Earlier blobs: finviz export `c4199bbdd4` (06:53 ET), join `e87a13bdd7` (06:53 ET), weather `055df1a548` (06:53 ET), actions `15938bbb78` (07:46 ET). GPRO and INSP are not frozen rows. The rebuild sold FEAM and VKTX and bought nothing. |

## What matches

The send-time tickets and the Webull acknowledgements match after the wire's skip rules: already held, never held, and the 09-24 hard-red sit. Webull added no names that were absent from that commit's HOT4 list.

The rebuilt $10k book does not match those paper orders on any of the four days. 09-22 has no shared ticker. 09-23 shares INDP on opposite sides. 09-24 shares FEAM as a sell the paper account did not hold. 09-25 has no rebuilt orders.

## Wire replay

The day-by-day build calls `webull_exec.size_hot4_tickets` and `webull_exec.size_hot4_sells` for `union_hot_n4_h1`. It does not keep a second copy of the held, unheld, or hard-red checks. Replaying the paper submit journals from 2026-09-22 onward, with open lots taken from the 2026-09-21 paper print (DELL, GME, UMC), reproduces each journal's tickets and skip kinds: GME held on 09-22, SECZ/GRAL/NUAI unheld on 09-23, hard-red buys and GLND/FEAM unheld on 09-24. The $10k research state files are not rewritten. 2026-09-25 has no submit journal in the tree.

## Dated file guard

`strategy_tickets.write` keeps the dated file once a paper submit journal exists or 09:30 ET has arrived. A later different body goes to the undated live copies and to `<date>_strategy_tickets_draft.json`, and the job fails. 09-22 through 09-24 in this tree are the send-time commits.
