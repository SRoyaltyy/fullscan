# Open gates on current books — should we apply #154 CLEARs?

_Generated 2026-09-08 · tip `e6fa4e33` / `OPEN_GATES_BOARD.md` · **research only** · live frozen._

## Plain English

If we overlaid the #154 confirmed gates on **today’s** stock-selection strats, the book would change like this. Gates are research overlays — not wired. Same-row DF/BB/BQ still abort. Native Finviz/join/stock_book window is ~16 weekdays, so sleeve fire wr here is **PROVISIONAL** (cannot hit ≥30 fires). The ≥30 + >55% call stays the long Yahoo analog: `vol_top8` ≈ weighted / join-ranked, `prior_green_top8` ≈ green pile.

After-fee H is the equal-weight day-mean minus 15 bp Futubull, not dollar-weighted. Live `flatten_robust` is not imported.

## What the gates cut

**avoid FQ** (the strongest confirmed): drop names whose **prior session H > +3%** (yesterday already ran from the open). Refill the hole from the leftover ranked list. That is the opposite of chasing continuation.

Other confirmed avoids cut bigger prior prints (`ER=+1` ≥+5%, `EP≥3%` two-day move) or recent washouts (`AH≥1` had a −5% day in 6). Elevates swap ≤2 core names for a leftover **Hammer** or **FR≥1** (liquid / volume-spike).

Ticket books (flatten) **cannot refill**. Avoid only drops the names the sleeve already took. Elevate does nothing useful (no leftover cash).

## flatten_robust

**Before:** live tickets as taken (post-8-13 BUY fills; io 16:00 + mover 09:30). No Excel FQ/ER/AH gate.

**After `avoid_FQ`:** drop tickets whose prior H > +3%. 2 fire day(s) on 30 tickets. Fire wr **FAIL** 50.0% (1/2 fires) — **cannot CLEAR** (n≪30). Long analog is not flatten: flatten is a ticket filter, not a ranked top-8.

Tickets FQ would have blocked:

- 2026-08-20: drop AG (+6.44%), CDE (+5.20%), HDSN (-2.18%), KGC (+8.48%), NFGC (+0.80%), WPM (+6.99%), AEM (+5.75%) · keep BHP, IAG, ABUS
- 2026-08-21: drop AU (-0.69%), AEM (-1.07%), ARCT (+23.40%) · keep AUPH, AUTL, CRDL, CRSP, CYPH, FUTU, GMAB

Those drops are **yesterday’s runners** sitting in io leftover or mover books. #153 already **KILL**’d blanket J-avoid on flatten (fights mover gap-up, 50% of 4 fires). `avoid_FQ` is a cousin: it also cuts names that already printed a green H. Do **not** treat a tiny-n fire print as an improve. Sleeve fire hit-rate under ≥30 + >55%: **does not improve to CLEAR** — only 2 fire days, and the 08-20 mover book would have lost its best runners.

## green-pile

**Before:** prior-day `*_green.json` tickers (afternoon stamp, PIT), top-8 as the morning green book.

**After `avoid_FQ`:** drop prior-H>+3% pile names; refill from the rest of the pile. 16 overlay days. Fire wr **FAIL** 25.0% (2/8 fires). Long analog `prior_green_top8` **CLEAR** 61.4% long / 63.3% y2025 — that is the ≥30 + >55% call, not this 16-day dump.

Names that drop: pile names that already ran +3% yesterday (often the same ‘all-green continuation’ the pile likes). Refills are lower-ranked pile names that did **not** print that H.

## weighted book / join books

**Before:** prior-day `stock_book` 1d buy (weighted, PIT) top-8; same-morning join ranked top-8. No FQ gate.

**After `avoid_FQ`:** same drop + refill. Weighted 16 days, fire wr **FAIL** 40.0% (2/5 fires). Join 16 days, fire wr **FAIL** 40.0% (2/5 fires). Neither prints >55% here. Weighted 1d buy is often only 6–16 names, so a drop can land with **no refill**. Long analog `vol_top8` **CLEAR** 60.3% long / 56.3% y2025.

Drops are yesterday’s +3% H names that the join/weight scores still liked (continuation / uptrend flags). Refills come from ranks 9–80 that did not print that H — usually quieter leftover names, not new runners.

## Other confirmed gates (short)

| gate | what changes | long analog (ranked) | native sleeves |
|---|---|---|---|
| `avoid_FQ` | yesterday H > +3% (change-from-open). Drop those runners; refill from leftover. | vol 60.3% (191/317); green 61.4% (191/311) | join 40.0% (2/5 fires) |
| `avoid_ER_p1` | prior H/I printed +5% (ER=+1). Stronger runner cut than FQ. | vol 59.3% (172/290); green 63.0% (184/292) | join 66.7% (2/3 fires) |
| `avoid_EP_ge03` | weighted |H[t−2]|/|H[t−1]| ≥ 3%. Recent big-move names. | vol 57.0% (259/454); green 62.4% (199/319) | join 66.7% (2/3 fires) |
| `avoid_AH_ge1` | ≥1 prior H ≤ −5% in 6d. Washout / already-dumped names. | vol 56.7% (253/446); green 56.8% (167/294) | join 66.7% (2/3 fires) |
| `elev_cap2_lag_hammer` | prior-bar Hammer (DF[t−1]). Swap ≤2 core names for leftover hammers. | vol 57.4% (97/169); green — (long not CLEAR) | join 50.0% (5/10 fires) |
| `elev_cap2_FR_ge1` | prior vol median >1M and/or G≥3. Elevate leftover liquid / volume-spike names. | vol 56.4% (155/275); green — (long not CLEAR) | join 25.0% (3/12 fires) |

Elevate on flatten is a no-op (no leftover to pull). Avoid-AH drops recent −5% washouts — different names than FQ. ER/EP are stricter runner cuts (overlap FQ).

## One real case — 2026-09-04 join top-8 + `avoid_FQ`

**Before:** HRMY, HALO, CDNA, WAY, PLMR, ONC, KKR, NU

**After:** CDNA, WAY, PLMR, ONC, KKR, COF, INCY, KRYS

Dropped (prior H>+3%): **HRMY, HALO, NU**. Refill from leftover: **COF, INCY, KRYS**.

| ticker | role | prior H | same-day H after fees |
|---|---|---:|---:|
| HRMY | drop | 3.8% | -2.6% |
| HALO | drop | 3.6% | 0.7% |
| NU | drop | 6.1% | -0.1% |
| COF | refill | 2.3% | 0.5% |
| INCY | refill | 1.9% | -1.4% |
| KRYS | refill | 0.9% | -0.1% |

That morning the rule book **won** the no-rule eight (after-fee mean +0.38% vs +0.26%). One day is not the bar.

Green-pile same morning: drop HRMY, HALO; add PCRX, KNSA.
Weighted 1d same morning: drop CEG, VAL; add —.
Flatten: no BUY tickets that session (or none tripped FQ).

## Would sleeve fire hit-rate CLEAR?

Native flatten / green / weighted / join **no** — n_fires stays under 30. The board’s long liquid ranked tape is the only place these gates already CLEAR ≥30 + >55%, and that analog is **not** the live flatten sleeve.

Do not wire. `avoid_FQ` is the one to keep watching if a later cut re-scores a material flatten tape.

## Source

Tip `e6fa4e33` · `OPEN_GATES_BOARD.md` confirmed set · clock lock unchanged (lag DF/DG/DH/BB/BQ/BU; same-row DF/BB/BQ abort). Research only. Live frozen.
