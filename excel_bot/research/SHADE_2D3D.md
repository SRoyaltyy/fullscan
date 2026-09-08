# Shade hex — 2d / 3d cumulative stacked I

_Generated 2026-09-08 · live `flatten_robust` frozen · Yahoo/rows A–F seed only · no cards · no live push._

## Plain English

Same-day H already **DEMOTEd** open shade (M mid `#95CA82`) on the expanded universe (#150). This beat re-cuts the **same open-only fills** against **2d and 3d cumulative** labels — not a new letter search.

**Family verdict: pending panel** — miner `mine_shade_2d3d.py` is clock-gated and ready. Full Yahoo expand + score is the next step on this branch.

**Cyrus (visual):** Deep green/red look predictive because the loudest paints are same-row outcome color: Excel CF paints H>5% `#95CA82` and H>0 pale `#DCEDD5` (I>3% mint) after the close — that is H/I itself, not an open forecast; the one open-knowable mid-green (M `#95CA82` from yesterday's H × IY) is not that paint.

### Labels (clock-clean, documented)

Same definitions as `mine_hi_horizon.labels_for` / `HI_HORIZON.md`:

- **2d cumulative** = `I_sum` horizon 2 = `(1+I[t])×(1+I[t+1]) − 1`
- **3d cumulative** = `I_sum` horizon 3 = `(1+I[t])×(1+I[t+1])×(1+I[t+2]) − 1`
- **Same-day H** = Excel H (close vs open) — **baseline compare only**
- Same-row H/I are **labels**, never features. Lags from rows above are fair. Miner asserts I_sum against `labels_for` per ticker.

HI_HORIZON also reports the H *print* at horizon-end (H[t+k−1]) as a separate non-cumulative label. That is **not** this search.

### Open-only gate (standing bar)

Source: `OPEN_SAME_ROW_LABELS.md` + `CLOCK_MAP.md` + `clock.py`. `excel_clock_gate.py` is not on this branch; the miner asserts `clock_map.json` `fill_mine_open` = A B C G J K L M O IR IS IT.

- Features at the open: only those 12 fills. This expand paints **M** (CF `IZ=1` from *prior-row* H and static IY). Known at 9:30.
- **Never** M's number `-(low-open)/open`. Close fills D E F H I N do not start a trade.
- Onset uses yesterday's M fill (lag). Soft-regime heat is the prior-5 mean of I (`[ei-5, ei)`). SPY tape is a ship-bar slice.
- `FILL_IDX` maps through `VISIBLE` (M=12). The 4.4k / +10.6% KEEP had `enumerate(OPEN_FILL)` so M read **H's fill** (H>5%). Aborted.

Ship bar: Futubull 0.15% long off the recipe and the buy-everyone book. Beat book **and** any-green parent on the same letter by ≥20 bp. Ghost: top-5 / drop-5 / July / day lottery / Q1. Both SPY tapes. Soft regimes if any KEEP.

Universe: same expand panel as #150/#151 — all split names × max Yahoo history (~5223, 2018→2026). A / G / K / L multi-shade and O mint are **not painted** on this expand (M-only). IR/IS/IT have no CF.

### H-fill diagnostic (close-knowable — not a KEEP path)

Excel CF on column H (first match):

| rule | hex | clock |
|---|---|---|
| H > 5% | `#95CA82` mid green | **close** — same-row H |
| H > 0 | `#DCEDD5` pale | **close** — same-row H |
| 0 ≥ H ≥ −2.9% | `#FF5050` | **close** |
| H < −3% | `#FFC7CE` pink | **close** (also I < −3%) |
| I > 3% | `#C6EFCE` mint | **close** — same-row I |

That is why deep/mid green on H *looks* like a predictor: the cell is painted with today's H. The leaked 4.4k KEEP was this fill mis-indexed as M.

### What this does not change

- Live `flatten_robust` is frozen. No card. No push.
- Same-day H shade DEMOTE from #150 stays demoted until this recut says otherwise.
- Standing five-cell light+green O ± AH/FR is not remine.
- Close-entry fills stay out of the open clock.

Research only. Live frozen.
