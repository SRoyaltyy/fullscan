# Shade hex + onset — open-knowable fills

_Generated 2026-09-08 · live `flatten_robust` frozen · Yahoo/rows A–F seed only · no cards · no live push._

## Plain English

The old color mine only asked whether a morning cell was green or not. Cyrus asked for **shades of green**: the stored fill hex. This beat inventories the locked open paints, then buys at the **open** when a cell is a specific hex / greener-than-X, versus plain green-on. If shade standing fails, it tries **onset** — the cell was red or off yesterday and flipped to this shade today. That is stricter than riding a standing green, and is not the same as the five-cell hysteresis light.

Labels are Excel **H** (intraday %) first — that is where the leftover same-day research still lived after soft-regime demoted the morning five-cell light+green O ± AH/FR family. Same-day **I** and a 2-day I stack are reported if cheap. Same-row H/I are never features. Close paints D/E/F/H/I/N never start this trade. IR/IS/IT are the STOCKHISTORY aliases of A/B/C; they have **no** CF of their own.

**Family verdict: null** — expanded-universe M mid is a **DEMOTE** (KEEP 0 / KILL 5 / THIN 0).

Shade hex and onset on open-knowable fills do not beat any-green or the book after fees, tapes, and the ghost bar.

The 4.4k / +10.6% KEEP was **H’s fill** (close-knowable `H≥5%`) mis-indexed as M. This rebuild is **5223** tickers × **2018-09-10 → 2026-09-04** (8.54M name-days). Real M mid `#95CA82` holdout **−0.19%** (n=320499) vs book −0.10% (−0.09 pp). Time split early −0.18% / late −0.41%. Soft-regime majority is N/A (no KEEP). Live frozen.

### Excel clock gate (source of truth)

Same-row open fills: **A B C G J K L M O IR IS IT**. Same-row numbers/text: the **44 `value_mine_open` cols** (never H/I). Lags of any letter are fair. OUT: M’s number, B/G/K/M/O numbers, D/E/F/H/I same-row, `core_score`. Docs: `OPEN_SAME_ROW_LABELS.md` + `CLOCK_MAP.md`. M `#95CA82` × 44/lag pairs: family pending — `SHADE_GATE.md`.

### Open-only gate (standing bar)

Every **feature** is knowable at that day's open — fill shade, number, and text. This is the Excel clock gate, not a slogan.

- Shades/fills at open: only **A B C G J K L M O IR IS IT**. Mid-M `#95CA82` expand stays **open-fill only**.
- Numbers/text at open: only the **44 `value_mine_open` cols**. Never same-row H/I.
- Lags: any letter from rows above is fair.
- OUT: M’s number, B/G/K/M/O numbers, D/E/F/H/I same-row, `core_score`.
- **M fill** `#95CA82` / ge15 is the feature. CF `IZ=1` reads *yesterday's* H and the static IY row (lag). Known at 9:30.
- **M's number** `-(low-open)/open` is same-day low → close-only. Never a feature. Never a gate.
- Close fills **D E F H I N** do not start a trade. Same-row H/I values, fills, text, and transforms are **labels only**.
- Onset uses yesterday's M fill (lag). Soft-regime heat is the prior-5 mean of I (`[ei-5, ei)`). SPY tape is a ship-bar slice, not a buy feature.
- `FILL_IDX` maps through `VISIBLE` (M=12). The 1000-grid / 4.4k KEEP had `enumerate(OPEN_FILL)` so M read **H's fill** (close-knowable `H≥5%`). That path is aborted.

**Ghost / name check: GHOST FAIL**

Standing M mid-green `#95CA82` / ge15 does not clear the ghost / name bar (holdout sign, leftover vs book/parent, or a handful). M mid `#95CA82` / ge15 **GHOST FAIL** — holdout -0.19% (n=320499), vs book -0.09 pp, vs parent +0.00 pp. Holdout top-5 -4.0% (GDC, SSM, NA, TSSI, BESS). Drop-5 leftover -0.20% (n=319875). M hex-onset **GHOST FAIL** — holdout -0.23% (n=250101), vs book -0.13 pp, vs parent -0.04 pp. Holdout top-5 -3.3% (GDC, SSM, NA, BESS, TSSI). Drop-5 leftover -0.23% (n=249606). O red→green **GHOST THIN** — holdout +0.00% (n=0), vs book +0.00 pp, vs parent +0.00 pp. Holdout top-5 0.0% (). Drop-5 leftover +0.00% (n=None). Do not talk wire.

Dumps **5223** tickers. Calendar days **2008**. Same-day H recipes: **KEEP 0** · **KILL 5** · **THIN 0**. Futubull 0.15% long is taken off the recipe and the buy-everyone book. Beat the book by ≥20 bp. Ghost bar: Q1 not red, five names ≤25% of P&L, July ≤40% of winning-month P&L, fattest day ≤25%. Both SPY tapes. Soft-regime heat is the open-knowable prior-5 mean of I (discovery terciles cold ≤ -0.37%, hot ≥ 0.39%).

### Inventory — which open fills have more than one green hex

CF rules in `model.json` (first matching rule wins). Dumps confirm which hexes actually fire.

| letter | CF green hexes | dump green hexes | multi-shade? | note |
|---|---|---|---|---|
| **A** | #3B7D23, #B8DCAB | — | yes | — |
| **B** | #95CA82 | — | no | — |
| **C** | — | — | no | peach only — not a green |
| **G** | #3B7D23, #B8DCAB | — | yes | — |
| **J** | #95CA82 | — | no | — |
| **K** | #3B7D23, #95CA82 | — | yes | — |
| **L** | #3B7D23, #C6EFCE | — | yes | — |
| **M** | #95CA82 | #95CA82 | no | — |
| **O** | #C6EFCE | — | no | — |
| **IR** | — | — | no | no CF; value alias of A |
| **IS** | — | — | no | no CF; value alias of B |
| **IT** | — | — | no | no CF; value alias of C |

CF-multi letters: **A, G, K, L**. Dump-multi letters (the ones that actually fire two greens): **none**. A / K / L / M have two greens in the dumps. G has two greens in CF but **no green fires** on this rebuild (purple/blue instead). J's CF green `#95CA82` is also absent here. O is a single mint `#C6EFCE` — shade-of-O is identical to green-O. B is one mid green `#95CA82`. C is peach. IR / IS / IT have zero CF rules.

Do **not** rehash prior-I heat-green. Soft-regime below is only a gate on shade KEEP candidates, not a new I-heat mine.


### Ghost / name check (harder than soft-regime majority)

Family KEEP already cleared the usual 25% top-5 / 40% July / 25% day-lottery bar. This cut asks whether a **handful of names** is the holdout print. Holdout-only top-5 (PASS ≤15%, FAIL >25%), drop top-5 leftover still beating the book and any-green M, July holdout ≤25% of winning-month P&L, fattest holdout day ≤15%, Q1 **on holdout names** not red, both SPY tapes on holdout, name-split (discovery vs holdout tickers) and time-split (cut 2026-05-01). Futubull 0.15% is on every print. Live stays frozen.

**Ghost verdict: GHOST FAIL**

Standing M mid-green `#95CA82` / ge15 does not clear the ghost / name bar (holdout sign, leftover vs book/parent, or a handful). M mid `#95CA82` / ge15 **GHOST FAIL** — holdout -0.19% (n=320499), vs book -0.09 pp, vs parent +0.00 pp. Holdout top-5 -4.0% (GDC, SSM, NA, TSSI, BESS). Drop-5 leftover -0.20% (n=319875). M hex-onset **GHOST FAIL** — holdout -0.23% (n=250101), vs book -0.13 pp, vs parent -0.04 pp. Holdout top-5 -3.3% (GDC, SSM, NA, BESS, TSSI). Drop-5 leftover -0.23% (n=249606). O red→green **GHOST THIN** — holdout +0.00% (n=0), vs book +0.00 pp, vs parent +0.00 pp. Holdout top-5 0.0% (). Drop-5 leftover +0.00% (n=None). Do not talk wire.

M mid `#95CA82` / ge15: **GHOST FAIL**. M hex-onset: **GHOST FAIL**. O red→green onset: **GHOST THIN**.

| recipe | holdout | vs book | vs parent | drop-5 leftover | holdout top-5 | July | day | Q1 holdout | SPY↑ | SPY↓ | handful? | ghost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `M_ge15` | -0.19% (n=320499) | -0.09 pp | +0.00 pp | -0.20% (n=319875) | -4.0% (GDC, SSM, NA, TSSI, BESS) | 0.0% | 2.7% | -0.20% (n=299468) | holdout spy_up +0.27% does not beat that tape's book | holdout spy_dn -0.74% | yes | **FAIL** |
| `M_hex_95CA82` | -0.19% (n=320499) | -0.09 pp | +0.00 pp | -0.20% (n=319875) | -4.0% (GDC, SSM, NA, TSSI, BESS) | 0.0% | 2.7% | -0.20% (n=299468) | holdout spy_up +0.27% does not beat that tape's book | holdout spy_dn -0.74% | yes | **FAIL** |
| `M_onset_hex_95CA82` | -0.23% (n=250101) | -0.13 pp | -0.04 pp | -0.23% (n=249606) | -3.3% (GDC, SSM, NA, BESS, TSSI) | 0.0% | 3.5% | -0.23% (n=234667) | holdout spy_up +0.26% does not beat that tape's book | holdout spy_dn -0.79% | yes | **FAIL** |
| `O_onset_red2green` | — | +0.00 pp | +0.00 pp | — | 0.0% () | 0.0% | 0.0% | — | — | — | no | **THIN** |

Holdout top names (P&L share of that recipe's holdout book):

| recipe | name | n | holdout avg | holdout P&L share |
|---|---|---:|---:|---:|
| `M_ge15` | GDC | 200 | +3.68% | -1.2% |
| `M_ge15` | SSM | 96 | +7.39% | -1.2% |
| `M_ge15` | NA | 85 | +3.96% | -0.6% |
| `M_ge15` | TSSI | 174 | +1.81% | -0.5% |
| `M_ge15` | BESS | 69 | +4.57% | -0.5% |
| `M_onset_hex_95CA82` | GDC | 150 | +4.27% | -1.1% |
| `M_onset_hex_95CA82` | SSM | 76 | +5.51% | -0.7% |
| `M_onset_hex_95CA82` | NA | 71 | +4.21% | -0.5% |
| `M_onset_hex_95CA82` | BESS | 56 | +4.61% | -0.5% |
| `M_onset_hex_95CA82` | TSSI | 142 | +1.81% | -0.5% |

M_ge15 and M_hex_95CA82 are the same trades (real M has one green hex). Drop-5 leftover is the holdout mean after removing the five fattest names. A red leftover is not a five-name ghost of a winner — there is no winner.

### Same-day H (primary)

| meaning | holdout | vs book | vs parent | Q1 | SPY↑ | SPY↓ | top-5 | July | verdict | why | code |
|---|---|---|---|---|---|---|---|---|---|---|---|
| morning cell M is mid-or-deep green (score ≥ 1.5) (known at 9:30) | -0.19% (n=320499) | -0.09 pp | +0.00 pp | -0.21% (n=746098) | +0.27% (n=356712) | -0.75% (n=302578) | -2% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_ge15` |
| morning cell M is hex #95CA82 (mid green) (known at 9:30) | -0.19% (n=320499) | -0.09 pp | +0.00 pp | -0.21% (n=746098) | +0.27% (n=356712) | -0.75% (n=302578) | -2% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_hex_95CA82` |
| morning cell M is highlighted any green (known at 9:30) | -0.19% (n=320499) | -0.09 pp | — | -0.21% (n=746098) | +0.27% (n=356712) | -0.75% (n=302578) | -2% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split | `M_green` |
| morning cell M was off or not green yesterday and is green today (known at 9:30) | -0.23% (n=250101) | -0.13 pp | -0.04 pp | -0.24% (n=584673) | +0.25% (n=269907) | -0.79% (n=246317) | -2% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_onset_green` |
| morning cell M flips onto hex #95CA82 (mid) today (known at 9:30) | -0.23% (n=250101) | -0.13 pp | -0.04 pp | -0.24% (n=584673) | +0.25% (n=269907) | -0.79% (n=246317) | -2% | 0% | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split,no_edge_vs_parent | `M_onset_hex_95CA82` |

### Same-day I / 2d stacked I (cheap report)

Not the search. Prior-I heat-green is not remine. These rows only say whether a shade KEEP on H also prints on I.

| label | best recipe | holdout | verdict | why | code |
|---|---|---|---|---|---|
| same-day I | morning cell M is highlighted any green (known at 9:30) | -0.18% (n=320499) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split | `M_green` |
| 2d stacked I | morning cell M is highlighted any green (known at 9:30) | -0.12% (n=320497) | **KILL** | disc_t,hold_t,disc_sign,hold_sign,lottery_day,tape_split,spy_regime,q3_sign,no_edge_vs_uncond,q1_sign,month_split | `M_green` |

### What this does not change

- Live `flatten_robust` is frozen. No card. No push.
- Soft-regime already **DEMOTEd** the morning five-cell light+green O ± AH/FR family (thin leftover same-day H only).
- Full-sheet ML (#149) stays family null vs the overnight-gap book.
- Prior-I heat-green is not remine.
- Close-entry fills stay out of the open clock.

Research only. Expanded panel is 2018-09 → 2026-09. Live frozen.
