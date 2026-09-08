# Shade hex + onset — open-knowable fills

_Generated 2026-09-08 · live `flatten_robust` frozen · Yahoo/rows A–F seed only · no cards · no live push._

## Plain English

The old color mine only asked whether a morning cell was green or not. Cyrus asked for **shades of green**: the stored fill hex. This beat inventories the locked open paints, then buys at the **open** when a cell is a specific hex / greener-than-X, versus plain green-on. If shade standing fails, it tries **onset** — the cell was red or off yesterday and flipped to this shade today. That is stricter than riding a standing green, and is not the same as the five-cell hysteresis light.

Labels are Excel **H** (intraday %) first — that is where the leftover same-day research still lived after soft-regime demoted the morning five-cell light+green O ± AH/FR family. Same-day **I** and a 2-day I stack are reported if cheap. Same-row H/I are never features. Close paints D/E/F/H/I/N never start this trade. IR/IS/IT are the STOCKHISTORY aliases of A/B/C; they have **no** CF of their own.

**Family verdict: KEEP**

Shade hex is not the same as green-on. Mid-green M (#95CA82 / score ≥ 1.5) and its onset beat any-green M and the book after fees, both tapes, and the ghost bar (`M_ge15`, `M_hex_95CA82`). Soft-regime majority holds on those recipes. Not a live wire.

Dumps **1000**. Name-days scored **170**. Same-day H recipes: **KEEP 24** · **KILL 59** · **THIN 5**. Futubull 0.15% long is taken off the recipe and the buy-everyone book. Beat the book by ≥20 bp. Ghost bar: Q1 not red, five names ≤25% of P&L, July ≤40% of winning-month P&L, fattest day ≤25%. Both SPY tapes. Soft-regime heat is the open-knowable prior-5 mean of I (discovery terciles cold ≤ -0.45%, hot ≥ 0.44%).

### Inventory — which open fills have more than one green hex

CF rules in `model.json` (first matching rule wins). Dumps confirm which hexes actually fire.

| letter | CF green hexes | dump green hexes | multi-shade? | note |
|---|---|---|---|---|
| **A** | #3B7D23, #B8DCAB | #B8DCAB, #3B7D23 | yes | — |
| **B** | #95CA82 | #95CA82 | no | — |
| **C** | — | — | no | peach only — not a green |
| **G** | #3B7D23, #B8DCAB | — | yes | — |
| **J** | #95CA82 | — | no | — |
| **K** | #3B7D23, #95CA82 | #B8DCAB, #3B7D23 | yes | — |
| **L** | #3B7D23, #C6EFCE | #3B7D23, #B8DCAB | yes | — |
| **M** | #95CA82 | #DCEDD5, #95CA82 | yes | — |
| **O** | #C6EFCE | #C6EFCE | no | — |
| **IR** | — | — | no | no CF; value alias of A |
| **IS** | — | — | no | no CF; value alias of B |
| **IT** | — | — | no | no CF; value alias of C |

CF-multi letters: **A, G, K, L**. Dump-multi letters (the ones that actually fire two greens): **A, K, L, M**. A / K / L / M have two greens in the dumps. G has two greens in CF but **no green fires** on this rebuild (purple/blue instead). J's CF green `#95CA82` is also absent here. O is a single mint `#C6EFCE` — shade-of-O is identical to green-O. B is one mid green `#95CA82`. C is peach. IR / IS / IT have zero CF rules.

Do **not** rehash prior-I heat-green. Soft-regime below is only a gate on shade KEEP candidates, not a new I-heat mine.

### Same-day H (primary)

| meaning | holdout | vs book | vs parent | Q1 | SPY↑ | SPY↓ | top-5 | July | verdict | why | code |
|---|---|---|---|---|---|---|---|---|---|---|---|
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on | +1.66% (n=3016) | +1.75 pp | — | +2.14% (n=1727) | +2.14% (n=3600) | +1.43% (n=2670) | 9% | 10% | **KEEP** | — | `light_on` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell M is mid-or-deep green (score ≥ 1.5) | +10.77% (n=363) | +10.85 pp | +7.58 pp | +10.69% (n=275) | +11.27% (n=537) | +10.26% (n=283) | 12% | 9% | **KEEP** | — | `light__M_ge15` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell M is hex #95CA82 (mid green) | +10.77% (n=363) | +10.85 pp | +7.58 pp | +10.69% (n=275) | +11.27% (n=537) | +10.26% (n=283) | 12% | 9% | **KEEP** | — | `light__M_hex_95CA82` |
| morning cell M is mid-or-deep green (score ≥ 1.5) (known at 9:30) | +10.57% (n=4423) | +10.66 pp | +7.86 pp | +9.68% (n=3900) | +10.30% (n=6306) | +10.22% (n=3346) | 7% | 10% | **KEEP** | — | `M_ge15` |
| morning cell M is hex #95CA82 (mid green) (known at 9:30) | +10.57% (n=4423) | +10.66 pp | +7.86 pp | +9.68% (n=3900) | +10.30% (n=6306) | +10.22% (n=3346) | 7% | 10% | **KEEP** | — | `M_hex_95CA82` |
| morning cell M flips onto hex #95CA82 (mid) today (known at 9:30) | +10.34% (n=3731) | +10.43 pp | +7.63 pp | +9.28% (n=3278) | +9.95% (n=5336) | +9.82% (n=2769) | 7% | 10% | **KEEP** | — | `M_onset_hex_95CA82` |
| morning cell O flips from red yesterday to green today (known at 9:30) | +7.21% (n=2502) | +7.30 pp | +1.52 pp | +5.87% (n=2301) | +6.52% (n=3977) | +6.39% (n=1650) | 12% | 11% | **KEEP** | — | `O_onset_red2green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell O is highlighted any green | +6.69% (n=670) | +6.77 pp | +5.02 pp | +7.41% (n=427) | +6.99% (n=1008) | +6.36% (n=494) | 10% | 10% | **KEEP** | — | `light__O_green` |
| morning cell O is highlighted any green (known at 9:30) | +5.69% (n=9687) | +5.78 pp | — | +5.41% (n=8111) | +5.41% (n=14728) | +5.74% (n=6541) | 5% | 10% | **KEEP** | — | `O_green` |
| morning cell K flips onto hex #3B7D23 (deep) today (known at 9:30) | +3.83% (n=3148) | +3.91 pp | +3.28 pp | +3.15% (n=1663) | +3.87% (n=4249) | +2.66% (n=2521) | 18% | 8% | **KEEP** | — | `K_onset_hex_3B7D23` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell K flips onto hex #3B7D23 (deep) today | +3.71% (n=413) | +3.80 pp | +2.21 pp | +3.95% (n=192) | +3.74% (n=549) | +2.97% (n=316) | 10% | 14% | **KEEP** | — | `light__K_onset_hex_3B7D23` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell M is highlighted any green | +3.19% (n=2035) | +3.27 pp | +1.52 pp | +3.62% (n=1225) | +3.66% (n=2536) | +2.96% (n=1763) | 7% | 11% | **KEEP** | — | `light__M_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell A was off or not green yesterday and is green today | +3.08% (n=457) | +3.16 pp | +1.41 pp | +3.86% (n=274) | +3.62% (n=636) | +2.61% (n=395) | 11% | 11% | **KEEP** | — | `light__A_onset_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell A flips from red yesterday to green today | +3.06% (n=456) | +3.15 pp | +1.40 pp | +3.61% (n=261) | +3.51% (n=623) | +2.61% (n=395) | 12% | 12% | **KEEP** | — | `light__A_onset_red2green` |
| morning cell M is highlighted any green (known at 9:30) | +2.71% (n=32247) | +2.80 pp | — | +2.64% (n=27815) | +2.85% (n=41184) | +2.52% (n=26420) | 4% | 11% | **KEEP** | — | `M_green` |
| morning cell K was off or not green yesterday and is green today (known at 9:30) | +2.65% (n=3528) | +2.74 pp | +2.11 pp | +2.09% (n=2103) | +2.60% (n=4986) | +1.81% (n=2624) | 23% | 8% | **KEEP** | — | `K_onset_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell B flips from red yesterday to green today | +2.59% (n=792) | +2.67 pp | +0.89 pp | +3.31% (n=469) | +2.72% (n=1074) | +2.34% (n=689) | 9% | 11% | **KEEP** | — | `light__B_onset_red2green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell K was off or not green yesterday and is green today | +2.51% (n=356) | +2.60 pp | +1.01 pp | +3.52% (n=176) | +2.85% (n=534) | +2.07% (n=245) | 12% | 9% | **KEEP** | — | `light__K_onset_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell K is mid-or-deep green (score ≥ 1.5) | +2.32% (n=1273) | +2.41 pp | +0.82 pp | +2.85% (n=631) | +2.99% (n=1521) | +2.06% (n=1102) | 13% | 10% | **KEEP** | — | `light__K_ge15` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell K is deep green (score ≥ 2.0) | +2.32% (n=1273) | +2.41 pp | +0.82 pp | +2.85% (n=631) | +2.99% (n=1521) | +2.06% (n=1102) | 13% | 10% | **KEEP** | — | `light__K_ge20` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell K is hex #3B7D23 (deep green) | +2.32% (n=1273) | +2.41 pp | +0.82 pp | +2.85% (n=631) | +2.99% (n=1521) | +2.06% (n=1102) | 13% | 10% | **KEEP** | — | `light__K_hex_3B7D23` |
| morning cell K is mid-or-deep green (score ≥ 1.5) (known at 9:30) | +1.56% (n=9165) | +1.64 pp | +1.01 pp | +1.35% (n=4592) | +1.99% (n=10976) | +0.95% (n=7718) | 17% | 7% | **KEEP** | — | `K_ge15` |
| morning cell K is deep green (score ≥ 2.0) (known at 9:30) | +1.56% (n=9165) | +1.64 pp | +1.01 pp | +1.35% (n=4592) | +1.99% (n=10976) | +0.95% (n=7718) | 17% | 7% | **KEEP** | — | `K_ge20` |
| morning cell K is hex #3B7D23 (deep green) (known at 9:30) | +1.56% (n=9165) | +1.64 pp | +1.01 pp | +1.35% (n=4592) | +1.99% (n=10976) | +0.95% (n=7718) | 17% | 7% | **KEEP** | — | `K_hex_3B7D23` |
| morning cell K is highlighted any green (known at 9:30) | +0.55% (n=23525) | +0.63 pp | — | +0.53% (n=11590) | +0.86% (n=28136) | +0.10% (n=21021) | 19% | 5% | **KEEP** | — | `K_green` |
| morning cell K flips from red yesterday to green today (known at 9:30) | +13.25% (n=360) | +13.34 pp | +12.70 pp | +5.53% (n=203) | +11.46% (n=510) | +6.37% (n=285) | 54% | 5% | **KILL** | hold_t,ticker_ghost | `K_onset_red2green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell M flips onto hex #95CA82 (mid) today | +8.60% (n=170) | +8.69 pp | +5.41 pp | +8.30% (n=106) | +8.39% (n=234) | +7.86% (n=117) | 7% | 10% | **THIN** | thin_disc | `light__M_onset_hex_95CA82` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell O is hex #C6EFCE (mint green) | +6.69% (n=670) | +6.77 pp | +0.00 pp | +7.41% (n=427) | +6.99% (n=1008) | +6.36% (n=494) | 10% | 10% | **KILL** | no_edge_vs_parent | `light__O_hex_C6EFCE` |
| morning cell O is hex #C6EFCE (mint green) (known at 9:30) | +5.69% (n=9687) | +5.78 pp | +0.00 pp | +5.41% (n=8111) | +5.41% (n=14728) | +5.74% (n=6541) | 5% | 10% | **KILL** | no_edge_vs_parent | `O_hex_C6EFCE` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell O was off or not green yesterday and is green today | +5.57% (n=279) | +5.66 pp | -1.11 pp | +6.04% (n=163) | +5.24% (n=377) | +5.13% (n=190) | 7% | 13% | **KILL** | no_edge_vs_parent | `light__O_onset_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell O flips onto hex #C6EFCE (mint) today | +5.57% (n=279) | +5.66 pp | -1.11 pp | +6.04% (n=163) | +5.24% (n=377) | +5.13% (n=190) | 7% | 13% | **KILL** | no_edge_vs_parent | `light__O_onset_hex_C6EFCE` |
| morning cell O was off or not green yesterday and is green today (known at 9:30) | +5.48% (n=7793) | +5.56 pp | -0.22 pp | +5.11% (n=6592) | +5.16% (n=11868) | +5.40% (n=5138) | 6% | 11% | **KILL** | no_edge_vs_parent | `O_onset_green` |
| morning cell O flips onto hex #C6EFCE (mint) today (known at 9:30) | +5.48% (n=7793) | +5.56 pp | -0.22 pp | +5.11% (n=6592) | +5.16% (n=11868) | +5.40% (n=5138) | 6% | 11% | **KILL** | no_edge_vs_parent | `O_onset_hex_C6EFCE` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell L flips onto hex #B8DCAB (pale) today | +5.19% (n=167) | +5.27 pp | +3.38 pp | +6.37% (n=136) | +5.45% (n=213) | +3.83% (n=123) | 17% | 6% | **THIN** | thin_disc | `light__L_onset_hex_B8DCAB` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell L is hex #B8DCAB (pale green) | +5.07% (n=197) | +5.16 pp | +3.27 pp | +6.49% (n=153) | +5.37% (n=249) | +3.83% (n=138) | 17% | 6% | **THIN** | thin_disc | `light__L_hex_B8DCAB` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell A is hex #B8DCAB (pale green) | +5.05% (n=163) | +5.13 pp | +3.38 pp | +6.24% (n=153) | +5.14% (n=243) | +3.35% (n=120) | 18% | 5% | **THIN** | thin_disc | `light__A_hex_B8DCAB` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell A flips onto hex #B8DCAB (pale) today | +4.34% (n=74) | +4.43 pp | +2.68 pp | +5.79% (n=58) | +5.20% (n=107) | +3.90% (n=55) | 23% | 9% | **THIN** | thin_disc,thin_hold | `light__A_onset_hex_B8DCAB` |
| morning cell M was off or not green yesterday and is green today (known at 9:30) | +2.85% (n=16937) | +2.93 pp | +0.13 pp | +2.67% (n=14767) | +2.97% (n=21925) | +2.50% (n=13811) | 5% | 12% | **KILL** | no_edge_vs_parent | `M_onset_green` |
| morning cell M flips from red yesterday to green today (known at 9:30) | +2.84% (n=16507) | +2.93 pp | +0.13 pp | +2.64% (n=13992) | +2.97% (n=21226) | +2.48% (n=13660) | 5% | 12% | **KILL** | no_edge_vs_parent | `M_onset_red2green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell M was off or not green yesterday and is green today | +2.59% (n=262) | +2.67 pp | -0.60 pp | +2.39% (n=159) | +2.42% (n=324) | +2.48% (n=205) | 9% | 12% | **KILL** | no_edge_vs_parent | `light__M_onset_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell M flips from red yesterday to green today | +2.59% (n=262) | +2.67 pp | -0.60 pp | +2.39% (n=159) | +2.38% (n=321) | +2.48% (n=205) | 9% | 12% | **KILL** | no_edge_vs_parent | `light__M_onset_red2green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell B was off or not green yesterday and is green today | +1.82% (n=1817) | +1.91 pp | +0.12 pp | +2.50% (n=981) | +2.10% (n=2260) | +1.55% (n=1617) | 7% | 11% | **KILL** | no_edge_vs_parent | `light__B_onset_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell B flips onto hex #95CA82 (mid) today | +1.82% (n=1817) | +1.91 pp | +0.12 pp | +2.50% (n=981) | +2.10% (n=2260) | +1.55% (n=1617) | 7% | 11% | **KILL** | no_edge_vs_parent | `light__B_onset_hex_95CA82` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell L was off or not green yesterday and is green today | +1.82% (n=1774) | +1.90 pp | +0.01 pp | +2.49% (n=1002) | +2.20% (n=2042) | +1.48% (n=1557) | 6% | 10% | **KILL** | no_edge_vs_parent | `light__L_onset_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell L is highlighted any green | +1.81% (n=2540) | +1.89 pp | +0.14 pp | +2.47% (n=1390) | +2.38% (n=3040) | +1.51% (n=2247) | 9% | 10% | **KILL** | no_edge_vs_parent | `light__L_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell M flips onto hex #DCEDD5 (pale) today | +1.78% (n=595) | +1.87 pp | -1.40 pp | +1.76% (n=370) | +1.90% (n=766) | +1.83% (n=520) | 3% | 11% | **KILL** | no_edge_vs_parent | `light__M_onset_hex_DCEDD5` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell B is highlighted any green | +1.70% (n=2881) | +1.79 pp | +0.03 pp | +2.19% (n=1662) | +2.19% (n=3452) | +1.43% (n=2584) | 9% | 10% | **KILL** | no_edge_vs_parent | `light__B_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell B is mid-or-deep green (score ≥ 1.5) | +1.70% (n=2881) | +1.79 pp | +0.00 pp | +2.19% (n=1662) | +2.19% (n=3452) | +1.43% (n=2584) | 9% | 10% | **KILL** | no_edge_vs_parent | `light__B_ge15` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell B is hex #95CA82 (mid green) | +1.70% (n=2881) | +1.79 pp | +0.00 pp | +2.19% (n=1662) | +2.19% (n=3452) | +1.43% (n=2584) | 9% | 10% | **KILL** | no_edge_vs_parent | `light__B_hex_95CA82` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell A is highlighted any green | +1.66% (n=3016) | +1.75 pp | +0.00 pp | +2.14% (n=1727) | +2.14% (n=3600) | +1.43% (n=2670) | 9% | 10% | **KILL** | no_edge_vs_parent | `light__A_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell A flips onto hex #3B7D23 (deep) today | +1.65% (n=2313) | +1.74 pp | -0.01 pp | +1.96% (n=1325) | +2.16% (n=2793) | +1.52% (n=2115) | 11% | 11% | **KILL** | no_edge_vs_parent | `light__A_onset_hex_3B7D23` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell M is hex #DCEDD5 (pale green) | +1.54% (n=1672) | +1.63 pp | -1.65 pp | +1.57% (n=950) | +1.61% (n=1999) | +1.56% (n=1480) | 2% | 14% | **KILL** | no_edge_vs_parent | `light__M_hex_DCEDD5` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell L is mid-or-deep green (score ≥ 1.5) | +1.53% (n=2343) | +1.62 pp | -0.27 pp | +1.97% (n=1237) | +2.11% (n=2791) | +1.36% (n=2109) | 11% | 11% | **KILL** | no_edge_vs_parent | `light__L_ge15` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell L is deep green (score ≥ 2.0) | +1.53% (n=2343) | +1.62 pp | -0.27 pp | +1.97% (n=1237) | +2.11% (n=2791) | +1.36% (n=2109) | 11% | 11% | **KILL** | no_edge_vs_parent | `light__L_ge20` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell L is hex #3B7D23 (deep green) | +1.53% (n=2343) | +1.62 pp | -0.27 pp | +1.97% (n=1237) | +2.11% (n=2791) | +1.36% (n=2109) | 11% | 11% | **KILL** | no_edge_vs_parent | `light__L_hex_3B7D23` |
| morning cell M flips onto hex #DCEDD5 (pale) today (known at 9:30) | +1.53% (n=15835) | +1.61 pp | -1.19 pp | +1.55% (n=13761) | +1.58% (n=20155) | +1.45% (n=13188) | 1% | 13% | **KILL** | no_edge_vs_parent | `M_onset_hex_DCEDD5` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell L flips onto hex #3B7D23 (deep) today | +1.51% (n=1662) | +1.60 pp | -0.30 pp | +1.90% (n=907) | +2.24% (n=1907) | +1.31% (n=1502) | 14% | 11% | **KILL** | no_edge_vs_parent | `light__L_onset_hex_3B7D23` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell K is highlighted any green | +1.51% (n=2611) | +1.59 pp | -0.16 pp | +1.80% (n=1335) | +1.97% (n=3137) | +1.33% (n=2263) | 11% | 10% | **KILL** | no_edge_vs_parent | `light__K_green` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell A is mid-or-deep green (score ≥ 1.5) | +1.47% (n=2853) | +1.56 pp | -0.19 pp | +1.75% (n=1574) | +1.93% (n=3357) | +1.34% (n=2550) | 10% | 11% | **KILL** | no_edge_vs_parent | `light__A_ge15` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell A is deep green (score ≥ 2.0) | +1.47% (n=2853) | +1.56 pp | -0.19 pp | +1.75% (n=1574) | +1.93% (n=3357) | +1.34% (n=2550) | 10% | 11% | **KILL** | no_edge_vs_parent | `light__A_ge20` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell A is hex #3B7D23 (deep green) | +1.47% (n=2853) | +1.56 pp | -0.19 pp | +1.75% (n=1574) | +1.93% (n=3357) | +1.34% (n=2550) | 10% | 11% | **KILL** | no_edge_vs_parent | `light__A_hex_3B7D23` |
| morning cell M is hex #DCEDD5 (pale green) (known at 9:30) | +1.46% (n=27824) | +1.55 pp | -1.25 pp | +1.49% (n=23915) | +1.51% (n=34878) | +1.41% (n=23074) | 1% | 13% | **KILL** | no_edge_vs_parent | `M_hex_DCEDD5` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell K is hex #B8DCAB (pale green) | +0.73% (n=1338) | +0.81 pp | -0.78 pp | +0.86% (n=704) | +1.01% (n=1616) | +0.63% (n=1161) | 8% | 10% | **KILL** | no_edge_vs_parent | `light__K_hex_B8DCAB` |
| the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell K flips onto hex #B8DCAB (pale) today | +0.56% (n=422) | +0.65 pp | -0.94 pp | +1.00% (n=215) | +1.11% (n=552) | +0.62% (n=344) | 13% | 7% | **KILL** | no_edge_vs_parent | `light__K_onset_hex_B8DCAB` |
| morning cell K flips onto hex #B8DCAB (pale) today (known at 9:30) | -0.05% (n=4584) | +0.04 pp | -0.60 pp | +0.10% (n=2376) | +0.18% (n=5749) | -0.38% (n=4066) | -28% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,month_split,no_edge_vs_parent | `K_onset_hex_B8DCAB` |
| morning cell A flips onto hex #B8DCAB (pale) today (known at 9:30) | -0.09% (n=10700) | -0.01 pp | +0.05 pp | -0.25% (n=8002) | +0.30% (n=13096) | -0.74% (n=9456) | -14% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `A_onset_hex_B8DCAB` |
| morning cell K is hex #B8DCAB (pale green) (known at 9:30) | -0.10% (n=14360) | -0.01 pp | -0.64 pp | -0.00% (n=6998) | +0.14% (n=17160) | -0.39% (n=13303) | -9% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `K_hex_B8DCAB` |
| morning cell A is hex #B8DCAB (pale green) (known at 9:30) | -0.13% (n=25481) | -0.04 pp | +0.01 pp | -0.25% (n=24251) | +0.27% (n=29319) | -0.61% (n=23279) | -8% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `A_hex_B8DCAB` |
| morning cell A was off or not green yesterday and is green today (known at 9:30) | -0.13% (n=6782) | -0.05 pp | +0.01 pp | -0.22% (n=5984) | +0.40% (n=8440) | -0.86% (n=5891) | -27% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `A_onset_green` |
| morning cell B is mid-or-deep green (score ≥ 1.5) (known at 9:30) | -0.14% (n=25273) | -0.05 pp | +0.00 pp | -0.22% (n=19315) | +0.26% (n=28206) | -0.56% (n=24141) | -14% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `B_ge15` |
| morning cell B is hex #95CA82 (mid green) (known at 9:30) | -0.14% (n=25273) | -0.05 pp | +0.00 pp | -0.22% (n=19315) | +0.26% (n=28206) | -0.56% (n=24141) | -14% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `B_hex_95CA82` |
| morning cell A is highlighted any green (known at 9:30) | -0.14% (n=42665) | -0.05 pp | — | -0.23% (n=32980) | +0.26% (n=48722) | -0.59% (n=39924) | -10% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split | `A_green` |
| morning cell B was off or not green yesterday and is green today (known at 9:30) | -0.15% (n=12553) | -0.06 pp | -0.01 pp | -0.20% (n=9953) | +0.24% (n=14777) | -0.67% (n=11566) | -13% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `B_onset_green` |
| morning cell B flips onto hex #95CA82 (mid) today (known at 9:30) | -0.15% (n=12553) | -0.06 pp | -0.01 pp | -0.20% (n=9953) | +0.24% (n=14777) | -0.67% (n=11566) | -13% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `B_onset_hex_95CA82` |
| morning cell A flips from red yesterday to green today (known at 9:30) | -0.15% (n=6396) | -0.06 pp | -0.01 pp | -0.32% (n=5057) | +0.40% (n=7511) | -0.85% (n=5887) | -22% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `A_onset_red2green` |
| morning cell L is mid-or-deep green (score ≥ 1.5) (known at 9:30) | -0.15% (n=15865) | -0.07 pp | +0.02 pp | -0.18% (n=8013) | +0.33% (n=18194) | -0.70% (n=15432) | -20% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `L_ge15` |
| morning cell L is deep green (score ≥ 2.0) (known at 9:30) | -0.15% (n=15865) | -0.07 pp | +0.02 pp | -0.18% (n=8013) | +0.33% (n=18194) | -0.70% (n=15432) | -20% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `L_ge20` |
| morning cell L is hex #3B7D23 (deep green) (known at 9:30) | -0.15% (n=15865) | -0.07 pp | +0.02 pp | -0.18% (n=8013) | +0.33% (n=18194) | -0.70% (n=15432) | -20% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `L_hex_3B7D23` |
| morning cell A is mid-or-deep green (score ≥ 1.5) (known at 9:30) | -0.16% (n=17184) | -0.07 pp | -0.02 pp | -0.18% (n=8729) | +0.26% (n=19403) | -0.58% (n=16645) | -24% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `A_ge15` |
| morning cell A is deep green (score ≥ 2.0) (known at 9:30) | -0.16% (n=17184) | -0.07 pp | -0.02 pp | -0.18% (n=8729) | +0.26% (n=19403) | -0.58% (n=16645) | -24% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `A_ge20` |

### Same-day I / 2d stacked I (cheap report)

Not the search. Prior-I heat-green is not remine. These rows only say whether a shade KEEP on H also prints on I.

| label | best recipe | holdout | verdict | why | code |
|---|---|---|---|---|---|
| same-day I | the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell M is mid-or-deep green (score ≥ 1.5) | +10.50% (n=363) | **KEEP** | — | `light__M_ge15` |
| 2d stacked I | the five morning cells (A, B, C, G, J) add up to a strong green (+5 or more) and the light first turns on, and morning cell M is mid-or-deep green (score ≥ 1.5) | +11.37% (n=363) | **KEEP** | — | `light__M_ge15` |

### Soft-regime (KEEP H candidates only)

Heat × SPY tape versus the same-cell book after fees. Not a new I-heat mine. Family verdict uses **per-recipe majority of the 9 cells**, not a dump of every cell.

#### Per-recipe majority

| recipe | KEEP | KILL | THIN | of 9 | slot |
|---|---:|---:|---:|---:|---|
| `M_green` | 9 | 0 | 0 | 9 | **KEEP** |
| `O_green` | 9 | 0 | 0 | 9 | **KEEP** |
| `M_ge15` | 8 | 1 | 0 | 9 | **KEEP** |
| `M_hex_95CA82` | 8 | 1 | 0 | 9 | **KEEP** |
| `M_onset_hex_95CA82` | 8 | 1 | 0 | 9 | **KEEP** |
| `O_onset_red2green` | 8 | 1 | 0 | 9 | **KEEP** |
| `light__M_green` | 8 | 0 | 1 | 9 | **KEEP** |
| `K_onset_hex_3B7D23` | 5 | 3 | 1 | 9 | **KEEP** |
| `K_ge15` | 4 | 4 | 1 | 9 | **REGIME-CONDITIONAL** |
| `K_ge20` | 4 | 4 | 1 | 9 | **REGIME-CONDITIONAL** |
| `K_hex_3B7D23` | 4 | 4 | 1 | 9 | **REGIME-CONDITIONAL** |
| `light__B_onset_red2green` | 4 | 2 | 3 | 9 | **DEMOTE** |
| `light__O_green` | 4 | 1 | 4 | 9 | **DEMOTE** |
| `K_green` | 3 | 6 | 0 | 9 | **DEMOTE** |
| `K_onset_green` | 3 | 6 | 0 | 9 | **DEMOTE** |
| `light__A_onset_green` | 3 | 2 | 4 | 9 | **DEMOTE** |
| `light__K_onset_hex_3B7D23` | 3 | 0 | 6 | 9 | **DEMOTE** |
| `light__A_onset_red2green` | 2 | 3 | 4 | 9 | **DEMOTE** |
| `light__K_ge15` | 2 | 4 | 3 | 9 | **DEMOTE** |
| `light__K_ge20` | 2 | 4 | 3 | 9 | **DEMOTE** |
| `light__K_hex_3B7D23` | 2 | 4 | 3 | 9 | **DEMOTE** |
| `light__M_ge15` | 2 | 1 | 6 | 9 | **DEMOTE** |
| `light__M_hex_95CA82` | 2 | 1 | 6 | 9 | **DEMOTE** |
| `light__K_onset_green` | 1 | 2 | 6 | 9 | **DEMOTE** |
| `light__L_hex_B8DCAB` | 1 | 1 | 7 | 9 | **REGIME-CONDITIONAL** |
| `light__M_onset_hex_95CA82` | 1 | 0 | 8 | 9 | **DEMOTE** |
| `light__A_hex_B8DCAB` | 0 | 1 | 8 | 9 | **DEMOTE** |
| `light__A_onset_hex_B8DCAB` | 0 | 0 | 9 | 9 | **DEMOTE** |
| `light__L_onset_hex_B8DCAB` | 0 | 1 | 8 | 9 | **DEMOTE** |

#### Cell dump

| recipe | heat | SPY | holdout | vs book | Q1 | top-5 | verdict | why |
|---|---|---|---|---|---|---|---|---|
| `K_ge15` | cold | up | +29.78% (n=145) | +28.98 pp | +7.69% (n=67) | 65% | **KILL** | ticker_ghost,lottery_day |
| `K_ge15` | cold | dn | +3.03% (n=92) | +3.91 pp | +3.39% (n=83) | 35% | **KILL** | ticker_ghost |
| `K_ge15` | cold | flat | +5.23% (n=28) | +5.82 pp | +6.32% (n=14) | 58% | **THIN** | thin,q1_thin,ticker_ghost |
| `K_ge15` | mixed | up | +1.48% (n=1101) | +1.27 pp | +1.71% (n=432) | 10% | **KEEP** | — |
| `K_ge15` | mixed | dn | +1.10% (n=733) | +1.54 pp | +1.09% (n=489) | 29% | **KILL** | ticker_ghost |
| `K_ge15` | mixed | flat | +0.95% (n=312) | +1.03 pp | +0.81% (n=130) | 19% | **KEEP** | — |
| `K_ge15` | hot | up | +1.30% (n=3336) | +1.00 pp | +1.55% (n=1216) | 10% | **KEEP** | — |
| `K_ge15` | hot | dn | +0.66% (n=2441) | +1.19 pp | +0.83% (n=1763) | 21% | **KEEP** | — |
| `K_ge15` | hot | flat | +0.86% (n=977) | +1.23 pp | +1.54% (n=398) | 26% | **KILL** | ticker_ghost |
| `K_ge20` | cold | up | +29.78% (n=145) | +28.98 pp | +7.69% (n=67) | 65% | **KILL** | ticker_ghost,lottery_day |
| `K_ge20` | cold | dn | +3.03% (n=92) | +3.91 pp | +3.39% (n=83) | 35% | **KILL** | ticker_ghost |
| `K_ge20` | cold | flat | +5.23% (n=28) | +5.82 pp | +6.32% (n=14) | 58% | **THIN** | thin,q1_thin,ticker_ghost |
| `K_ge20` | mixed | up | +1.48% (n=1101) | +1.27 pp | +1.71% (n=432) | 10% | **KEEP** | — |
| `K_ge20` | mixed | dn | +1.10% (n=733) | +1.54 pp | +1.09% (n=489) | 29% | **KILL** | ticker_ghost |
| `K_ge20` | mixed | flat | +0.95% (n=312) | +1.03 pp | +0.81% (n=130) | 19% | **KEEP** | — |
| `K_ge20` | hot | up | +1.30% (n=3336) | +1.00 pp | +1.55% (n=1216) | 10% | **KEEP** | — |
| `K_ge20` | hot | dn | +0.66% (n=2441) | +1.19 pp | +0.83% (n=1763) | 21% | **KEEP** | — |
| `K_ge20` | hot | flat | +0.86% (n=977) | +1.23 pp | +1.54% (n=398) | 26% | **KILL** | ticker_ghost |
| `K_green` | cold | up | +5.23% (n=944) | +4.43 pp | +1.96% (n=411) | 51% | **KILL** | ticker_ghost,lottery_day |
| `K_green` | cold | dn | +0.15% (n=614) | +1.03 pp | +0.74% (n=421) | 36% | **KILL** | ticker_ghost |
| `K_green` | cold | flat | +1.61% (n=163) | +2.19 pp | +1.79% (n=75) | 39% | **KILL** | ticker_ghost |
| `K_green` | mixed | up | +0.59% (n=3935) | +0.37 pp | +0.65% (n=1587) | 8% | **KEEP** | — |
| `K_green` | mixed | dn | +0.17% (n=2913) | +0.61 pp | +0.31% (n=1771) | 46% | **KILL** | ticker_ghost |
| `K_green` | mixed | flat | +0.35% (n=972) | +0.43 pp | +0.39% (n=447) | 17% | **KEEP** | — |
| `K_green` | hot | up | +0.61% (n=6762) | +0.31 pp | +0.83% (n=2413) | 11% | **KEEP** | — |
| `K_green` | hot | dn | -0.03% (n=5239) | +0.49 pp | +0.13% (n=3651) | -411% | **KILL** | hold_sign,lottery_day |
| `K_green` | hot | flat | +0.26% (n=1983) | +0.63 pp | +0.86% (n=814) | 49% | **KILL** | ticker_ghost |
| `K_hex_3B7D23` | cold | up | +29.78% (n=145) | +28.98 pp | +7.69% (n=67) | 65% | **KILL** | ticker_ghost,lottery_day |
| `K_hex_3B7D23` | cold | dn | +3.03% (n=92) | +3.91 pp | +3.39% (n=83) | 35% | **KILL** | ticker_ghost |
| `K_hex_3B7D23` | cold | flat | +5.23% (n=28) | +5.82 pp | +6.32% (n=14) | 58% | **THIN** | thin,q1_thin,ticker_ghost |
| `K_hex_3B7D23` | mixed | up | +1.48% (n=1101) | +1.27 pp | +1.71% (n=432) | 10% | **KEEP** | — |
| `K_hex_3B7D23` | mixed | dn | +1.10% (n=733) | +1.54 pp | +1.09% (n=489) | 29% | **KILL** | ticker_ghost |
| `K_hex_3B7D23` | mixed | flat | +0.95% (n=312) | +1.03 pp | +0.81% (n=130) | 19% | **KEEP** | — |
| `K_hex_3B7D23` | hot | up | +1.30% (n=3336) | +1.00 pp | +1.55% (n=1216) | 10% | **KEEP** | — |
| `K_hex_3B7D23` | hot | dn | +0.66% (n=2441) | +1.19 pp | +0.83% (n=1763) | 21% | **KEEP** | — |
| `K_hex_3B7D23` | hot | flat | +0.86% (n=977) | +1.23 pp | +1.54% (n=398) | 26% | **KILL** | ticker_ghost |
| `K_onset_green` | cold | up | +13.63% (n=325) | +12.83 pp | +3.57% (n=159) | 62% | **KILL** | ticker_ghost,lottery_day |
| `K_onset_green` | cold | dn | +1.95% (n=158) | +2.83 pp | +2.01% (n=138) | 31% | **KILL** | ticker_ghost |
| `K_onset_green` | cold | flat | +2.39% (n=55) | +2.98 pp | +4.03% (n=21) | 56% | **KILL** | ticker_ghost |
| `K_onset_green` | mixed | up | +1.47% (n=872) | +1.26 pp | +1.64% (n=454) | 13% | **KEEP** | — |
| `K_onset_green` | mixed | dn | +1.31% (n=435) | +1.75 pp | +1.18% (n=335) | 38% | **KILL** | ticker_ghost |
| `K_onset_green` | mixed | flat | +0.82% (n=190) | +0.90 pp | +0.70% (n=59) | 29% | **KILL** | ticker_ghost |
| `K_onset_green` | hot | up | +1.83% (n=854) | +1.53 pp | +2.41% (n=435) | 12% | **KEEP** | — |
| `K_onset_green` | hot | dn | +1.34% (n=454) | +1.87 pp | +2.31% (n=429) | 21% | **KEEP** | — |
| `K_onset_green` | hot | flat | +1.67% (n=185) | +2.04 pp | +3.40% (n=73) | 30% | **KILL** | ticker_ghost |
| `K_onset_hex_3B7D23` | cold | up | +31.27% (n=138) | +30.47 pp | +8.10% (n=64) | 66% | **KILL** | ticker_ghost,lottery_day |
| `K_onset_hex_3B7D23` | cold | dn | +3.66% (n=88) | +4.54 pp | +3.57% (n=79) | 31% | **KILL** | ticker_ghost |
| `K_onset_hex_3B7D23` | cold | flat | +4.90% (n=26) | +5.48 pp | +7.16% (n=12) | 62% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `K_onset_hex_3B7D23` | mixed | up | +2.35% (n=629) | +2.14 pp | +2.65% (n=258) | 12% | **KEEP** | — |
| `K_onset_hex_3B7D23` | mixed | dn | +2.05% (n=363) | +2.49 pp | +1.67% (n=289) | 32% | **KILL** | ticker_ghost |
| `K_onset_hex_3B7D23` | mixed | flat | +1.32% (n=166) | +1.40 pp | +0.93% (n=63) | 23% | **KEEP** | — |
| `K_onset_hex_3B7D23` | hot | up | +2.96% (n=944) | +2.66 pp | +3.60% (n=347) | 7% | **KEEP** | — |
| `K_onset_hex_3B7D23` | hot | dn | +2.27% (n=575) | +2.79 pp | +3.32% (n=454) | 18% | **KEEP** | — |
| `K_onset_hex_3B7D23` | hot | flat | +3.38% (n=219) | +3.75 pp | +3.74% (n=97) | 23% | **KEEP** | — |
| `M_ge15` | cold | up | +12.84% (n=1110) | +12.04 pp | +9.42% (n=925) | 15% | **KEEP** | — |
| `M_ge15` | cold | dn | +9.50% (n=537) | +10.38 pp | +9.47% (n=580) | 7% | **KEEP** | — |
| `M_ge15` | cold | flat | +9.17% (n=191) | +9.76 pp | +9.40% (n=160) | 11% | **KEEP** | — |
| `M_ge15` | mixed | up | +8.95% (n=495) | +8.74 pp | +9.18% (n=451) | 5% | **KEEP** | — |
| `M_ge15` | mixed | dn | +8.23% (n=251) | +8.68 pp | +9.25% (n=270) | 15% | **KEEP** | — |
| `M_ge15` | mixed | flat | +8.72% (n=127) | +8.80 pp | +8.59% (n=173) | 8% | **KILL** | lottery_day |
| `M_ge15` | hot | up | +10.30% (n=901) | +10.01 pp | +10.13% (n=590) | 7% | **KEEP** | — |
| `M_ge15` | hot | dn | +11.14% (n=564) | +11.67 pp | +10.93% (n=544) | 8% | **KEEP** | — |
| `M_ge15` | hot | flat | +10.07% (n=247) | +10.44 pp | +9.60% (n=207) | 11% | **KEEP** | — |
| `M_green` | cold | up | +3.91% (n=5606) | +3.11 pp | +3.24% (n=4713) | 10% | **KEEP** | — |
| `M_green` | cold | dn | +2.74% (n=3536) | +3.62 pp | +2.74% (n=3857) | 4% | **KEEP** | — |
| `M_green` | cold | flat | +2.77% (n=1288) | +3.35 pp | +3.01% (n=944) | 6% | **KEEP** | — |
| `M_green` | mixed | up | +1.93% (n=5829) | +1.72 pp | +2.03% (n=4922) | 2% | **KEEP** | — |
| `M_green` | mixed | dn | +1.64% (n=3768) | +2.08 pp | +1.75% (n=3953) | 5% | **KEEP** | — |
| `M_green` | mixed | flat | +1.88% (n=1655) | +1.96 pp | +2.17% (n=1642) | 3% | **KEEP** | — |
| `M_green` | hot | up | +3.04% (n=5456) | +2.74 pp | +3.13% (n=3281) | 4% | **KEEP** | — |
| `M_green` | hot | dn | +3.04% (n=3560) | +3.57 pp | +3.11% (n=3342) | 5% | **KEEP** | — |
| `M_green` | hot | flat | +2.87% (n=1549) | +3.24 pp | +3.02% (n=1161) | 6% | **KEEP** | — |
| `M_hex_95CA82` | cold | up | +12.84% (n=1110) | +12.04 pp | +9.42% (n=925) | 15% | **KEEP** | — |
| `M_hex_95CA82` | cold | dn | +9.50% (n=537) | +10.38 pp | +9.47% (n=580) | 7% | **KEEP** | — |
| `M_hex_95CA82` | cold | flat | +9.17% (n=191) | +9.76 pp | +9.40% (n=160) | 11% | **KEEP** | — |
| `M_hex_95CA82` | mixed | up | +8.95% (n=495) | +8.74 pp | +9.18% (n=451) | 5% | **KEEP** | — |
| `M_hex_95CA82` | mixed | dn | +8.23% (n=251) | +8.68 pp | +9.25% (n=270) | 15% | **KEEP** | — |
| `M_hex_95CA82` | mixed | flat | +8.72% (n=127) | +8.80 pp | +8.59% (n=173) | 8% | **KILL** | lottery_day |
| `M_hex_95CA82` | hot | up | +10.30% (n=901) | +10.01 pp | +10.13% (n=590) | 7% | **KEEP** | — |
| `M_hex_95CA82` | hot | dn | +11.14% (n=564) | +11.67 pp | +10.93% (n=544) | 8% | **KEEP** | — |
| `M_hex_95CA82` | hot | flat | +10.07% (n=247) | +10.44 pp | +9.60% (n=207) | 11% | **KEEP** | — |
| `M_onset_hex_95CA82` | cold | up | +12.95% (n=1026) | +12.15 pp | +9.31% (n=852) | 16% | **KEEP** | — |
| `M_onset_hex_95CA82` | cold | dn | +9.35% (n=501) | +10.23 pp | +9.37% (n=519) | 7% | **KEEP** | — |
| `M_onset_hex_95CA82` | cold | flat | +9.05% (n=175) | +9.63 pp | +9.14% (n=150) | 11% | **KEEP** | — |
| `M_onset_hex_95CA82` | mixed | up | +8.99% (n=439) | +8.78 pp | +8.91% (n=407) | 5% | **KEEP** | — |
| `M_onset_hex_95CA82` | mixed | dn | +7.90% (n=221) | +8.34 pp | +8.80% (n=232) | 16% | **KEEP** | — |
| `M_onset_hex_95CA82` | mixed | flat | +8.65% (n=107) | +8.73 pp | +8.48% (n=157) | 8% | **KILL** | lottery_day |
| `M_onset_hex_95CA82` | hot | up | +9.62% (n=662) | +9.32 pp | +9.89% (n=433) | 5% | **KEEP** | — |
| `M_onset_hex_95CA82` | hot | dn | +10.45% (n=410) | +10.98 pp | +9.61% (n=373) | 8% | **KEEP** | — |
| `M_onset_hex_95CA82` | hot | flat | +9.32% (n=190) | +9.69 pp | +8.99% (n=155) | 12% | **KEEP** | — |
| `O_green` | cold | up | +6.61% (n=2564) | +5.81 pp | +5.20% (n=2139) | 12% | **KEEP** | — |
| `O_green` | cold | dn | +5.63% (n=994) | +6.51 pp | +5.46% (n=1098) | 6% | **KEEP** | — |
| `O_green` | cold | flat | +4.97% (n=433) | +5.56 pp | +5.66% (n=299) | 10% | **KEEP** | — |
| `O_green` | mixed | up | +4.36% (n=1367) | +4.15 pp | +4.69% (n=1065) | 4% | **KEEP** | — |
| `O_green` | mixed | dn | +4.56% (n=580) | +5.00 pp | +4.73% (n=642) | 10% | **KEEP** | — |
| `O_green` | mixed | flat | +4.68% (n=288) | +4.77 pp | +5.05% (n=313) | 6% | **KEEP** | — |
| `O_green` | hot | up | +5.53% (n=2014) | +5.23 pp | +5.76% (n=1227) | 6% | **KEEP** | — |
| `O_green` | hot | dn | +6.77% (n=985) | +7.29 pp | +6.74% (n=926) | 7% | **KEEP** | — |
| `O_green` | hot | flat | +5.89% (n=462) | +6.26 pp | +5.41% (n=402) | 9% | **KEEP** | — |
| `O_onset_red2green` | cold | up | +9.01% (n=1057) | +8.21 pp | +5.77% (n=970) | 23% | **KEEP** | — |
| `O_onset_red2green` | cold | dn | +6.33% (n=393) | +7.21 pp | +5.50% (n=410) | 9% | **KEEP** | — |
| `O_onset_red2green` | cold | flat | +4.40% (n=174) | +4.99 pp | +5.61% (n=151) | 13% | **KEEP** | — |
| `O_onset_red2green` | mixed | up | +4.60% (n=248) | +4.39 pp | +5.07% (n=174) | 8% | **KEEP** | — |
| `O_onset_red2green` | mixed | dn | +4.40% (n=97) | +4.84 pp | +4.65% (n=105) | 11% | **KEEP** | — |
| `O_onset_red2green` | mixed | flat | +5.56% (n=44) | +5.64 pp | +6.03% (n=77) | 21% | **KILL** | lottery_day |
| `O_onset_red2green` | hot | up | +6.35% (n=282) | +6.06 pp | +7.36% (n=203) | 9% | **KEEP** | — |
| `O_onset_red2green` | hot | dn | +8.62% (n=136) | +9.15 pp | +7.79% (n=133) | 19% | **KEEP** | — |
| `O_onset_red2green` | hot | flat | +6.93% (n=71) | +7.30 pp | +5.56% (n=78) | 20% | **KEEP** | — |
| `light__A_hex_B8DCAB` | cold | up | +4.40% (n=20) | +3.60 pp | +6.25% (n=19) | 32% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__A_hex_B8DCAB` | cold | dn | +1.71% (n=10) | +2.59 pp | +2.97% (n=9) | 77% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__A_hex_B8DCAB` | cold | flat | +5.27% (n=7) | +5.85 pp | +2.82% (n=8) | 86% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__A_hex_B8DCAB` | mixed | up | +3.96% (n=19) | +3.75 pp | +4.62% (n=12) | 35% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__A_hex_B8DCAB` | mixed | dn | +3.71% (n=5) | +4.15 pp | +0.70% (n=4) | 113% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__A_hex_B8DCAB` | mixed | flat | +2.46% (n=4) | +2.54 pp | +3.83% (n=2) | 90% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__A_hex_B8DCAB` | hot | up | +6.23% (n=51) | +5.94 pp | +10.92% (n=38) | 29% | **KILL** | ticker_ghost |
| `light__A_hex_B8DCAB` | hot | dn | +3.51% (n=29) | +4.04 pp | +4.73% (n=39) | 33% | **THIN** | thin,ticker_ghost |
| `light__A_hex_B8DCAB` | hot | flat | +8.72% (n=18) | +9.10 pp | +5.52% (n=22) | 46% | **THIN** | thin,ticker_ghost,lottery_day |
| `light__A_onset_green` | cold | up | +2.05% (n=41) | +1.25 pp | +4.72% (n=23) | 24% | **KEEP** | — |
| `light__A_onset_green` | cold | dn | -0.11% (n=26) | +0.77 pp | +2.02% (n=26) | 82% | **THIN** | thin,hold_sign,ticker_ghost |
| `light__A_onset_green` | cold | flat | +3.81% (n=13) | +4.39 pp | +1.66% (n=6) | 68% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__A_onset_green` | mixed | up | +1.50% (n=48) | +1.28 pp | +3.79% (n=19) | 21% | **KEEP** | q1_thin |
| `light__A_onset_green` | mixed | dn | +2.09% (n=30) | +2.53 pp | +2.31% (n=29) | 57% | **THIN** | thin,ticker_ghost |
| `light__A_onset_green` | mixed | flat | +4.97% (n=8) | +5.05 pp | +0.77% (n=5) | 52% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__A_onset_green` | hot | up | +4.10% (n=156) | +3.80 pp | +5.50% (n=65) | 20% | **KEEP** | — |
| `light__A_onset_green` | hot | dn | +2.49% (n=94) | +3.01 pp | +4.02% (n=80) | 27% | **KILL** | ticker_ghost |
| `light__A_onset_green` | hot | flat | +5.56% (n=41) | +5.93 pp | +3.03% (n=21) | 57% | **KILL** | ticker_ghost |
| `light__A_onset_hex_B8DCAB` | cold | up | +3.16% (n=11) | +2.36 pp | +6.45% (n=9) | 49% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__A_onset_hex_B8DCAB` | cold | dn | +0.64% (n=7) | +1.52 pp | +1.15% (n=5) | 180% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__A_onset_hex_B8DCAB` | cold | flat | +1.90% (n=6) | +2.48 pp | +2.19% (n=5) | 102% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__A_onset_hex_B8DCAB` | mixed | up | +2.96% (n=8) | +2.75 pp | +4.90% (n=5) | 58% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__A_onset_hex_B8DCAB` | mixed | dn | +3.47% (n=4) | +3.91 pp | -0.95% (n=2) | 143% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__A_onset_hex_B8DCAB` | mixed | flat | +6.23% (n=1) | +6.31 pp | +4.17% (n=1) | 100% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__A_onset_hex_B8DCAB` | hot | up | +3.97% (n=19) | +3.67 pp | +10.25% (n=11) | 39% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__A_onset_hex_B8DCAB` | hot | dn | +2.84% (n=12) | +3.37 pp | +6.24% (n=15) | 51% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__A_onset_hex_B8DCAB` | hot | flat | +19.53% (n=6) | +19.90 pp | +5.55% (n=5) | 79% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__A_onset_red2green` | cold | up | +1.90% (n=40) | +1.10 pp | +3.89% (n=19) | 25% | **KILL** | q1_thin,ticker_ghost |
| `light__A_onset_red2green` | cold | dn | -0.11% (n=26) | +0.77 pp | +2.02% (n=26) | 82% | **THIN** | thin,hold_sign,ticker_ghost |
| `light__A_onset_red2green` | cold | flat | +3.81% (n=13) | +4.39 pp | +1.66% (n=6) | 68% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__A_onset_red2green` | mixed | up | +1.50% (n=48) | +1.28 pp | +3.41% (n=18) | 22% | **KEEP** | q1_thin |
| `light__A_onset_red2green` | mixed | dn | +2.09% (n=30) | +2.53 pp | +2.31% (n=29) | 57% | **THIN** | thin,ticker_ghost |
| `light__A_onset_red2green` | mixed | flat | +4.97% (n=8) | +5.05 pp | +0.77% (n=5) | 52% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__A_onset_red2green` | hot | up | +4.10% (n=156) | +3.80 pp | +5.03% (n=57) | 20% | **KEEP** | — |
| `light__A_onset_red2green` | hot | dn | +2.49% (n=94) | +3.01 pp | +4.02% (n=80) | 27% | **KILL** | ticker_ghost |
| `light__A_onset_red2green` | hot | flat | +5.56% (n=41) | +5.93 pp | +3.03% (n=21) | 57% | **KILL** | ticker_ghost |
| `light__B_onset_red2green` | cold | up | +1.94% (n=63) | +1.14 pp | +3.08% (n=25) | 20% | **KEEP** | — |
| `light__B_onset_red2green` | cold | dn | +0.37% (n=35) | +1.26 pp | +2.81% (n=34) | 47% | **THIN** | thin,ticker_ghost |
| `light__B_onset_red2green` | cold | flat | +3.07% (n=13) | +3.66 pp | +2.01% (n=9) | 64% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__B_onset_red2green` | mixed | up | +1.42% (n=70) | +1.21 pp | +1.98% (n=36) | 23% | **KEEP** | — |
| `light__B_onset_red2green` | mixed | dn | +1.95% (n=45) | +2.39 pp | +2.43% (n=39) | 41% | **KILL** | ticker_ghost |
| `light__B_onset_red2green` | mixed | flat | +3.06% (n=17) | +3.14 pp | +2.14% (n=13) | 40% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__B_onset_red2green` | hot | up | +3.21% (n=285) | +2.91 pp | +4.28% (n=130) | 13% | **KEEP** | — |
| `light__B_onset_red2green` | hot | dn | +2.11% (n=184) | +2.64 pp | +3.38% (n=146) | 23% | **KEEP** | — |
| `light__B_onset_red2green` | hot | flat | +4.15% (n=80) | +4.52 pp | +3.15% (n=37) | 45% | **KILL** | ticker_ghost |
| `light__K_ge15` | cold | up | +8.51% (n=7) | +7.71 pp | +8.62% (n=7) | 49% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__K_ge15` | cold | dn | -1.52% (n=7) | -0.64 pp | +4.81% (n=4) | 124% | **THIN** | thin,hold_sign,no_edge_vs_book,q1_thin,ticker_ghost |
| `light__K_ge15` | cold | flat | +18.49% (n=3) | +19.07 pp | — | 101% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__K_ge15` | mixed | up | +2.69% (n=85) | +2.48 pp | +2.69% (n=42) | 22% | **KEEP** | — |
| `light__K_ge15` | mixed | dn | +1.35% (n=73) | +1.80 pp | +2.84% (n=54) | 37% | **KILL** | ticker_ghost |
| `light__K_ge15` | mixed | flat | +1.70% (n=42) | +1.78 pp | +2.18% (n=12) | 34% | **KILL** | q1_thin,ticker_ghost |
| `light__K_ge15` | hot | up | +2.53% (n=522) | +2.23 pp | +3.44% (n=184) | 25% | **KILL** | ticker_ghost |
| `light__K_ge15` | hot | dn | +1.77% (n=386) | +2.30 pp | +2.29% (n=277) | 17% | **KEEP** | — |
| `light__K_ge15` | hot | flat | +3.02% (n=148) | +3.40 pp | +3.17% (n=51) | 33% | **KILL** | ticker_ghost |
| `light__K_ge20` | cold | up | +8.51% (n=7) | +7.71 pp | +8.62% (n=7) | 49% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__K_ge20` | cold | dn | -1.52% (n=7) | -0.64 pp | +4.81% (n=4) | 124% | **THIN** | thin,hold_sign,no_edge_vs_book,q1_thin,ticker_ghost |
| `light__K_ge20` | cold | flat | +18.49% (n=3) | +19.07 pp | — | 101% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__K_ge20` | mixed | up | +2.69% (n=85) | +2.48 pp | +2.69% (n=42) | 22% | **KEEP** | — |
| `light__K_ge20` | mixed | dn | +1.35% (n=73) | +1.80 pp | +2.84% (n=54) | 37% | **KILL** | ticker_ghost |
| `light__K_ge20` | mixed | flat | +1.70% (n=42) | +1.78 pp | +2.18% (n=12) | 34% | **KILL** | q1_thin,ticker_ghost |
| `light__K_ge20` | hot | up | +2.53% (n=522) | +2.23 pp | +3.44% (n=184) | 25% | **KILL** | ticker_ghost |
| `light__K_ge20` | hot | dn | +1.77% (n=386) | +2.30 pp | +2.29% (n=277) | 17% | **KEEP** | — |
| `light__K_ge20` | hot | flat | +3.02% (n=148) | +3.40 pp | +3.17% (n=51) | 33% | **KILL** | ticker_ghost |
| `light__K_hex_3B7D23` | cold | up | +8.51% (n=7) | +7.71 pp | +8.62% (n=7) | 49% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__K_hex_3B7D23` | cold | dn | -1.52% (n=7) | -0.64 pp | +4.81% (n=4) | 124% | **THIN** | thin,hold_sign,no_edge_vs_book,q1_thin,ticker_ghost |
| `light__K_hex_3B7D23` | cold | flat | +18.49% (n=3) | +19.07 pp | — | 101% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__K_hex_3B7D23` | mixed | up | +2.69% (n=85) | +2.48 pp | +2.69% (n=42) | 22% | **KEEP** | — |
| `light__K_hex_3B7D23` | mixed | dn | +1.35% (n=73) | +1.80 pp | +2.84% (n=54) | 37% | **KILL** | ticker_ghost |
| `light__K_hex_3B7D23` | mixed | flat | +1.70% (n=42) | +1.78 pp | +2.18% (n=12) | 34% | **KILL** | q1_thin,ticker_ghost |
| `light__K_hex_3B7D23` | hot | up | +2.53% (n=522) | +2.23 pp | +3.44% (n=184) | 25% | **KILL** | ticker_ghost |
| `light__K_hex_3B7D23` | hot | dn | +1.77% (n=386) | +2.30 pp | +2.29% (n=277) | 17% | **KEEP** | — |
| `light__K_hex_3B7D23` | hot | flat | +3.02% (n=148) | +3.40 pp | +3.17% (n=51) | 33% | **KILL** | ticker_ghost |
| `light__K_onset_green` | cold | up | +2.02% (n=28) | +1.22 pp | +4.74% (n=11) | 34% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__K_onset_green` | cold | dn | +1.54% (n=13) | +2.42 pp | +3.67% (n=9) | 60% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__K_onset_green` | cold | flat | +5.56% (n=5) | +6.15 pp | — | 93% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__K_onset_green` | mixed | up | +2.00% (n=58) | +1.78 pp | +4.41% (n=20) | 27% | **KILL** | ticker_ghost |
| `light__K_onset_green` | mixed | dn | +1.20% (n=24) | +1.64 pp | +3.83% (n=21) | 61% | **THIN** | thin,ticker_ghost |
| `light__K_onset_green` | mixed | flat | +1.64% (n=14) | +1.73 pp | +2.82% (n=4) | 65% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__K_onset_green` | hot | up | +2.94% (n=134) | +2.65 pp | +3.59% (n=55) | 20% | **KEEP** | — |
| `light__K_onset_green` | hot | dn | +1.38% (n=60) | +1.91 pp | +2.48% (n=50) | 31% | **KILL** | ticker_ghost |
| `light__K_onset_green` | hot | flat | +7.26% (n=20) | +7.63 pp | +5.61% (n=6) | 60% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__K_onset_hex_3B7D23` | cold | up | +8.51% (n=7) | +7.71 pp | +8.62% (n=7) | 49% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__K_onset_hex_3B7D23` | cold | dn | +3.82% (n=5) | +4.70 pp | +5.90% (n=3) | 77% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__K_onset_hex_3B7D23` | cold | flat | +13.79% (n=2) | +14.38 pp | — | 100% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__K_onset_hex_3B7D23` | mixed | up | +3.29% (n=52) | +3.08 pp | +4.73% (n=21) | 24% | **KEEP** | — |
| `light__K_onset_hex_3B7D23` | mixed | dn | +1.50% (n=34) | +1.94 pp | +4.04% (n=25) | 43% | **THIN** | thin,ticker_ghost |
| `light__K_onset_hex_3B7D23` | mixed | flat | +2.31% (n=21) | +2.39 pp | +2.87% (n=4) | 47% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__K_onset_hex_3B7D23` | hot | up | +3.96% (n=170) | +3.67 pp | +4.09% (n=62) | 15% | **KEEP** | — |
| `light__K_onset_hex_3B7D23` | hot | dn | +2.77% (n=86) | +3.29 pp | +2.87% (n=62) | 16% | **KEEP** | — |
| `light__K_onset_hex_3B7D23` | hot | flat | +6.80% (n=36) | +7.17 pp | +4.62% (n=8) | 52% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__L_hex_B8DCAB` | cold | up | +4.78% (n=13) | +3.98 pp | +7.01% (n=14) | 53% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__L_hex_B8DCAB` | cold | dn | +1.69% (n=8) | +2.58 pp | +2.62% (n=9) | 71% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__L_hex_B8DCAB` | cold | flat | +9.19% (n=7) | +9.78 pp | +3.09% (n=7) | 83% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__L_hex_B8DCAB` | mixed | up | +3.40% (n=14) | +3.19 pp | +5.98% (n=7) | 39% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__L_hex_B8DCAB` | mixed | dn | +2.22% (n=8) | +2.67 pp | +0.58% (n=4) | 146% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__L_hex_B8DCAB` | mixed | flat | +3.50% (n=4) | +3.58 pp | +3.83% (n=2) | 75% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__L_hex_B8DCAB` | hot | up | +5.53% (n=77) | +5.23 pp | +10.48% (n=41) | 25% | **KEEP** | — |
| `light__L_hex_B8DCAB` | hot | dn | +4.19% (n=43) | +4.72 pp | +5.30% (n=45) | 34% | **KILL** | ticker_ghost |
| `light__L_hex_B8DCAB` | hot | flat | +7.57% (n=23) | +7.94 pp | +5.37% (n=24) | 46% | **THIN** | thin,ticker_ghost |
| `light__L_onset_hex_B8DCAB` | cold | up | +4.38% (n=10) | +3.58 pp | +6.70% (n=11) | 59% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__L_onset_hex_B8DCAB` | cold | dn | +1.69% (n=8) | +2.58 pp | +2.62% (n=9) | 73% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__L_onset_hex_B8DCAB` | cold | flat | +6.47% (n=6) | +7.06 pp | +2.19% (n=5) | 98% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__L_onset_hex_B8DCAB` | mixed | up | +2.55% (n=10) | +2.34 pp | +5.98% (n=7) | 48% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__L_onset_hex_B8DCAB` | mixed | dn | +2.22% (n=8) | +2.67 pp | +0.58% (n=4) | 146% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__L_onset_hex_B8DCAB` | mixed | flat | +4.25% (n=2) | +4.34 pp | +4.17% (n=1) | 97% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__L_onset_hex_B8DCAB` | hot | up | +5.97% (n=67) | +5.67 pp | +10.26% (n=37) | 28% | **KILL** | ticker_ghost |
| `light__L_onset_hex_B8DCAB` | hot | dn | +4.40% (n=37) | +4.92 pp | +5.42% (n=42) | 37% | **THIN** | thin,ticker_ghost |
| `light__L_onset_hex_B8DCAB` | hot | flat | +8.20% (n=19) | +8.58 pp | +5.12% (n=20) | 52% | **THIN** | thin,ticker_ghost,lottery_day |
| `light__M_ge15` | cold | up | +9.43% (n=21) | +8.63 pp | +10.01% (n=18) | 18% | **THIN** | thin,q1_thin |
| `light__M_ge15` | cold | dn | +7.61% (n=10) | +8.49 pp | +8.57% (n=13) | 27% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__M_ge15` | cold | flat | +16.00% (n=4) | +16.59 pp | +5.58% (n=2) | 72% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__M_ge15` | mixed | up | +8.48% (n=30) | +8.27 pp | +8.25% (n=22) | 17% | **THIN** | thin |
| `light__M_ge15` | mixed | dn | +8.03% (n=16) | +8.47 pp | +10.62% (n=17) | 36% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__M_ge15` | mixed | flat | +8.45% (n=8) | +8.53 pp | +7.21% (n=4) | 35% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__M_ge15` | hot | up | +11.62% (n=148) | +11.32 pp | +11.61% (n=91) | 22% | **KEEP** | — |
| `light__M_ge15` | hot | dn | +10.66% (n=86) | +11.19 pp | +11.48% (n=79) | 16% | **KEEP** | — |
| `light__M_ge15` | hot | flat | +12.09% (n=40) | +12.46 pp | +9.73% (n=29) | 27% | **KILL** | ticker_ghost |
| `light__M_green` | cold | up | +3.27% (n=110) | +2.47 pp | +5.10% (n=45) | 12% | **KEEP** | — |
| `light__M_green` | cold | dn | +2.89% (n=50) | +3.77 pp | +3.84% (n=55) | 18% | **KEEP** | — |
| `light__M_green` | cold | flat | +4.54% (n=23) | +5.12 pp | +3.15% (n=14) | 40% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__M_green` | mixed | up | +2.12% (n=270) | +1.91 pp | +2.38% (n=137) | 8% | **KEEP** | — |
| `light__M_green` | mixed | dn | +1.76% (n=190) | +2.20 pp | +2.28% (n=157) | 14% | **KEEP** | — |
| `light__M_green` | mixed | flat | +1.94% (n=82) | +2.02 pp | +1.67% (n=42) | 14% | **KEEP** | — |
| `light__M_green` | hot | up | +3.98% (n=638) | +3.68 pp | +4.81% (n=288) | 15% | **KEEP** | — |
| `light__M_green` | hot | dn | +3.23% (n=481) | +3.76 pp | +3.57% (n=396) | 10% | **KEEP** | — |
| `light__M_green` | hot | flat | +3.76% (n=191) | +4.13 pp | +4.32% (n=91) | 18% | **KEEP** | — |
| `light__M_hex_95CA82` | cold | up | +9.43% (n=21) | +8.63 pp | +10.01% (n=18) | 18% | **THIN** | thin,q1_thin |
| `light__M_hex_95CA82` | cold | dn | +7.61% (n=10) | +8.49 pp | +8.57% (n=13) | 27% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__M_hex_95CA82` | cold | flat | +16.00% (n=4) | +16.59 pp | +5.58% (n=2) | 72% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__M_hex_95CA82` | mixed | up | +8.48% (n=30) | +8.27 pp | +8.25% (n=22) | 17% | **THIN** | thin |
| `light__M_hex_95CA82` | mixed | dn | +8.03% (n=16) | +8.47 pp | +10.62% (n=17) | 36% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__M_hex_95CA82` | mixed | flat | +8.45% (n=8) | +8.53 pp | +7.21% (n=4) | 35% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__M_hex_95CA82` | hot | up | +11.62% (n=148) | +11.32 pp | +11.61% (n=91) | 22% | **KEEP** | — |
| `light__M_hex_95CA82` | hot | dn | +10.66% (n=86) | +11.19 pp | +11.48% (n=79) | 16% | **KEEP** | — |
| `light__M_hex_95CA82` | hot | flat | +12.09% (n=40) | +12.46 pp | +9.73% (n=29) | 27% | **KILL** | ticker_ghost |
| `light__M_onset_hex_95CA82` | cold | up | +9.57% (n=14) | +8.77 pp | +9.65% (n=7) | 25% | **THIN** | thin,q1_thin |
| `light__M_onset_hex_95CA82` | cold | dn | +8.02% (n=8) | +8.90 pp | +7.97% (n=7) | 38% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__M_onset_hex_95CA82` | cold | flat | +15.48% (n=2) | +16.07 pp | +6.00% (n=1) | 100% | **THIN** | thin,q1_thin,ticker_ghost,lottery_day |
| `light__M_onset_hex_95CA82` | mixed | up | +8.78% (n=18) | +8.57 pp | +8.42% (n=15) | 28% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__M_onset_hex_95CA82` | mixed | dn | +6.44% (n=7) | +6.88 pp | +7.58% (n=7) | 40% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__M_onset_hex_95CA82` | mixed | flat | +9.28% (n=3) | +9.36 pp | +7.21% (n=4) | 46% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__M_onset_hex_95CA82` | hot | up | +8.50% (n=60) | +8.20 pp | +8.40% (n=31) | 11% | **KEEP** | — |
| `light__M_onset_hex_95CA82` | hot | dn | +8.24% (n=36) | +8.76 pp | +8.67% (n=24) | 21% | **THIN** | thin |
| `light__M_onset_hex_95CA82` | hot | flat | +8.88% (n=22) | +9.25 pp | +7.37% (n=10) | 26% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__O_green` | cold | up | +5.91% (n=47) | +5.11 pp | +8.33% (n=20) | 16% | **KEEP** | — |
| `light__O_green` | cold | dn | +3.06% (n=16) | +3.94 pp | +5.89% (n=25) | 26% | **THIN** | thin,ticker_ghost |
| `light__O_green` | cold | flat | +9.62% (n=8) | +10.21 pp | +4.33% (n=5) | 59% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__O_green` | mixed | up | +5.14% (n=70) | +4.93 pp | +5.96% (n=38) | 12% | **KEEP** | — |
| `light__O_green` | mixed | dn | +4.53% (n=35) | +4.97 pp | +6.25% (n=35) | 26% | **THIN** | thin,ticker_ghost |
| `light__O_green` | mixed | flat | +5.42% (n=17) | +5.50 pp | +4.46% (n=6) | 26% | **THIN** | thin,q1_thin,ticker_ghost |
| `light__O_green` | hot | up | +7.05% (n=281) | +6.76 pp | +8.29% (n=138) | 19% | **KEEP** | — |
| `light__O_green` | hot | dn | +7.23% (n=136) | +7.76 pp | +7.74% (n=119) | 14% | **KEEP** | — |
| `light__O_green` | hot | flat | +8.36% (n=60) | +8.73 pp | +7.10% (n=41) | 25% | **KILL** | ticker_ghost |

### What this does not change

- Live `flatten_robust` is frozen. No card. No push.
- Soft-regime already **DEMOTEd** the morning five-cell light+green O ± AH/FR family (thin leftover same-day H only).
- Full-sheet ML (#149) stays family null vs the overnight-gap book.
- Prior-I heat-green is not remine.
- Close-entry fills stay out of the open clock.

Research only. One 2026 regime.
