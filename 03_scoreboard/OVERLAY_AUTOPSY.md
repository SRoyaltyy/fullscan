# Overlay autopsy — avoid / elevate / expand

Kid: Stop buying the rotting apples. Pull the good apples we left on the bottom of the barrel. New recipes stay in the practice kitchen.

_Generated 2026-09-06T12:14:37.884896-04:00 · research only · live `flatten_robust` untouched._

Leak clock: features = **prior** Elite + prior AB + morning weather. 1d panel outcome = same-day `Change from Open` (never a gate). Book-gap fwd is the committed 1w gap scan. Paper fwd is closed round-trip after fees.

## Cyrus bar

| Goal | Kid | What we score |
|---|---|---|
| **Avoid** | Don't buy the ones that go bad. | Optional `avoid_veto` |
| **Elevate** | Rescue names we ranked 'meh' that then won. | Optional `elevate_bump` |
| **Expand** | New formulas we never wired. | vectorbt / OpenBB / sidecars only |

Theme Radar: **high `Forward P/E` fades both tapes**. Cheap / Magic Formula is **not** an auto long.

## Ranked findings

1. **Avoid that cleared both tapes:** `radar_high_fpe` / `avoid_veto` (n=4827, xs=-0.09, up xs=-0.03, down xs=-0.19). High `Forward P/E` ≥ 35 on the **prior** Elite file. Optional column only — not a live gate.
2. **`radar_hot` (d_RSI≥5 and d_mcap≥3%) failed both-tape** (n=2311, down xs=+0.35). Do not OR it into `avoid_veto`.
3. **`elevate_bump` did not clear both-tape** (n=142, up xs=-0.12, down n=24 xs=+1.00). Keep as a research sticker. Down-tape n is borderline (≥20).
4. **Cheap FPE is not a rescue overlay.** `radar_cheap_fpe` has a small both-tape xs (+0.15, n=17897, ~35% of the panel). That is 'not expensive', not 'high-conviction we ranked mediocre'. Theme Radar: cheap ≠ auto long.
5. **Magic Formula `mf_flag` failed both-tape** (up xs=-0.07, down xs=+0.12). Do not elevate on EY+ROIC alone.
6. **Bad buys:** 175 paper closed losers in 1d_top, 1d_size, 3d_top, 3d_size + 30 book-gap worst buys. Worst names (VERI −25%, AEVA −21%, RXT −21%, ACMR −14%) were join-positive or silent-AB, not CANSLIM. Two of the worst (BTBT FPE 152, INDI FPE 212) would have printed `avoid_veto`.
7. **Missed rockets:** outweighed=20 (elevate-shaped) · gated_out=25 (micro gate — expand, not a rank miss) · blind=11. REAX +853% was outweighed **and** high-FPE — the surviving avoid would have skipped a winner. That cost is why this stays optional.
8. **Expand only:** vectorbt, OpenBB/MDA, qlib/FinRL/AlphaSift/Vibe-Trading. flatten_live blotters are thin (7 start days) — not a second autopsy sample.
9. **Thin-n / data caveats:** 08-14 d_RSI often missing (no prior-prior RSI). Some d_mcap prints look like unit/corporate-action jumps (APPS +270%). Lookback 🔵/🚨/fade columns are empty on early books. 08-27 morning weather is unknown in this run.

Panel: **50445** liquid name-days · sessions 2026-08-13 → 2026-09-05 · base 1d mean **-0.13** · hit **42.2%**.

Both-tape = same-sign excess on realized SPY-up **and** SPY-down days, each cell n≥20. Otherwise **thin-n** — do not promote.

## 1. Avoid — rule scoreboard (liquid panel)

| rule | n | hit | mean 1d | xs vs base | up n / xs | down n / xs | both-tape |
|---|---:|---:|---:|---:|---:|---:|---|
| `avoid_veto` | 4827 | 41.5% | -0.22 | -0.09 | 1764 / -0.03 | 1269 / -0.19 | YES |
| `radar_high_fpe` | 4827 | 41.5% | -0.22 | -0.09 | 1764 / -0.03 | 1269 / -0.19 | YES |
| `radar_hot` | 2311 | 45.7% | -0.12 | +0.01 | 801 / -0.14 | 788 / +0.35 | NO |

High `Forward P/E` must fade **both** tapes to stay an avoid. If `radar_cheap_fpe` or `mf_flag` prints a *positive* both-tape elevate, ignore it — that is the cheap≠auto-long warning.

### Bad buys we actually took

Paper `1d_top, 1d_size, 3d_top, 3d_size` closed losers (fwd < 0, n=175) plus book-gap worst buys (1w, n=30). Showing the worst 15 by fwd.

| date | ticker | fwd | src | class | FPE | d_RSI | d_mcap | MF | CS | avoid | elev | AB | join | peer | 🔵 | 🚨 | fade | morn |
|---|---|---:|---|---|---:|---:|---:|:-:|:-:|:-:|:-:|---:|---:|---:|:-:|:-:|:-:|---|
| 2026-08-14 | `VERI` | -25.30 | book_gaps | bought | — | — | -34.30 |  |  |  |  | +0.00 | +0.55 | +0.00 |  |  |  | up |
| 2026-08-14 | `AEVA` | -21.43 | book_gaps | bought | — | — | +65.04 |  |  |  |  | +0.00 | +0.53 | +0.00 |  |  |  | up |
| 2026-08-14 | `RXT` | -21.14 | book_gaps | bought | +30.96 | — | +175.63 |  |  |  |  | +0.00 | +0.53 | +0.00 |  |  |  | up |
| 2026-08-14 | `WOLF` | -18.97 | book_gaps | bought | — | — | +18.42 |  |  |  |  | +0.00 | +0.55 | +0.00 |  |  |  | up |
| 2026-08-27 | `ACMR` | -13.75 | book_gaps | bought | +24.93 | +1.39 | +1.05 |  |  |  |  | +0.76 | +0.95 | +0.52 |  |  |  | unknown |
| 2026-08-14 | `TLN` | -13.31 | book_gaps | bought | +11.15 | — | +3.44 |  |  |  |  | +0.00 | +0.37 | +0.00 |  |  |  | up |
| 2026-08-17 | `NB` | -13.31 | paper:3d_size | bought | — | +1.61 | +1.73 |  |  |  |  | — | +0.17 | — |  |  |  | up |
| 2026-08-14 | `BTBT` | -13.28 | paper:3d_size | bought | +152.50 | — | +0.98 |  |  | Y |  | — | +0.55 | — |  |  |  | up |
| 2026-08-14 | `TLN` | -12.98 | paper:1d_top | bought | +11.15 | — | +3.44 |  |  |  |  | — | +0.37 | — |  |  |  | up |
| 2026-08-14 | `APPS` | -12.62 | book_gaps | bought | +13.36 | — | +270.14 |  |  |  |  | +0.00 | +0.53 | +0.00 |  |  |  | up |
| 2026-08-14 | `BAND` | -11.95 | book_gaps | bought | +28.25 | — | +123.25 |  |  |  |  | +0.00 | +0.53 | +0.00 |  |  |  | up |
| 2026-08-14 | `INDI` | -11.89 | book_gaps | bought | +211.98 | — | +12.75 |  |  | Y |  | +0.00 | +0.55 | +0.00 |  |  |  | up |
| 2026-08-14 | `TLN` | -11.69 | paper:3d_top | bought | +11.15 | — | +3.44 |  |  |  |  | — | +0.37 | — |  |  |  | up |
| 2026-08-27 | `ERO` | -11.30 | book_gaps | bought | +8.18 | +2.11 | +1.29 |  |  |  |  | +0.98 | +0.99 | +0.79 |  |  |  | unknown |
| 2026-08-14 | `NRG` | -10.40 | book_gaps | bought | +11.25 | — | -26.12 |  |  |  |  | +0.00 | +0.23 | +0.00 |  |  |  | up |

## 2. Elevate — rule scoreboard (liquid panel)

| rule | n | hit | mean 1d | xs vs base | up n / xs | down n / xs | both-tape |
|---|---:|---:|---:|---:|---:|---:|---|
| `elevate_bump` | 142 | 46.5% | +0.07 | +0.20 | 71 / -0.12 | 24 / +1.00 | NO |
| `canslim_flag` | 268 | 46.6% | -0.21 | -0.08 | 101 / -0.35 | 44 / +0.19 | NO |
| `mf_flag` | 822 | 47.0% | +0.02 | +0.15 | 303 / -0.07 | 218 / +0.12 | NO |
| `radar_cheap_fpe` | 17897 | 44.3% | +0.02 | +0.15 | 6660 / +0.08 | 4693 / +0.11 | YES |

`elevate_bump` = CANSLIM **and** not Theme-Radar avoid **and** (AB `P01=1` or `ab_score`≥8.0). `mf_flag` / cheap FPE alone never bump.

### Missed rockets (not top-ranked / not bought, then won big)

Book-gap missed movers with 1w fwd ≥ 10%. **outweighed** = elevate candidates (signals existed, rank buried them). **gated_out** = micro/mcap gate — expand, not a ranker miss. **blind** = every input silent.

| date | ticker | fwd | src | class | FPE | d_RSI | d_mcap | MF | CS | avoid | elev | AB | join | peer | 🔵 | 🚨 | fade | morn |
|---|---|---:|---|---|---:|---:|---:|:-:|:-:|:-:|:-:|---:|---:|---:|:-:|:-:|:-:|---|
| 2026-08-19 | `REAX` | +853.05 | book_gaps | outweighed | +64.97 | +3.40 | +5.65 |  |  | Y |  | +0.56 | -0.53 | +0.82 |  |  |  | down |
| 2026-08-19 | `ASST` | +45.96 | book_gaps | outweighed | +23.32 | +0.67 | +1.25 |  |  |  |  | -0.64 | -0.58 | +0.98 |  |  |  | down |
| 2026-08-19 | `ANF` | +40.11 | book_gaps | outweighed | +9.08 | +1.38 | +1.05 |  |  |  |  | +0.24 | -0.08 | -0.36 |  |  |  | down |
| 2026-08-19 | `TRON` | +32.72 | book_gaps | outweighed | — | -1.26 | -1.33 |  |  |  |  | +0.00 | -0.46 | +0.23 |  |  |  | down |
| 2026-08-19 | `CRML` | +29.50 | book_gaps | outweighed | — | -2.26 | -3.09 |  |  |  |  | -0.64 | -0.54 | -0.73 |  |  |  | down |
| 2026-08-19 | `TMC` | +26.20 | book_gaps | outweighed | — | -0.05 | -0.26 |  |  |  |  | -0.76 | -0.65 | -0.94 |  |  |  | down |
| 2026-08-21 | `CRM` | +22.39 | book_gaps | outweighed | +13.26 | +0.05 | +0.01 |  |  |  |  | +0.93 | -0.64 | +0.29 |  |  |  | up |
| 2026-08-27 | `MMED` | +22.19 | book_gaps | outweighed | +28.08 | -6.80 | -3.41 |  |  |  |  | +0.55 | -0.15 | +0.00 |  |  |  | unknown |
| 2026-08-27 | `FMC` | +21.76 | book_gaps | outweighed | +6.83 | +4.04 | +2.81 |  |  |  |  | +0.55 | +0.96 | +0.08 |  |  |  | unknown |
| 2026-08-27 | `CNH` | +21.08 | book_gaps | outweighed | +18.16 | -1.41 | -0.66 |  |  |  |  | +0.96 | +0.94 | +0.87 |  |  |  | unknown |
| 2026-08-21 | `RZLV` | +20.73 | book_gaps | outweighed | — | -1.48 | -1.82 |  |  |  |  | -0.55 | -0.98 | -0.82 |  |  |  | up |
| 2026-08-27 | `SMMT` | +19.54 | book_gaps | outweighed | — | +2.17 | +2.14 |  |  |  |  | -0.46 | -0.95 | +0.81 |  |  |  | unknown |
| 2026-08-21 | `ASST` | +19.32 | book_gaps | outweighed | +28.26 | +4.75 | +7.36 |  |  |  |  | -0.46 | +0.14 | +1.00 |  |  |  | up |
| 2026-08-27 | `SLBT` | +19.32 | book_gaps | outweighed | — | +2.61 | +4.89 |  |  |  |  | -0.55 | -0.90 | +0.00 |  |  |  | unknown |
| 2026-08-27 | `SID` | +18.87 | book_gaps | outweighed | — | +0.98 | +0.94 |  |  |  |  | +0.55 | +0.56 | +0.97 |  |  |  | unknown |

Gap classes: outweighed=20 · gated_out=25 · blind=11.

## 3. Expand — stay research

vectorbt / OpenBB / MarketDataApp / qlib / FinRL / AlphaSift / Vibe-Trading do **not** get avoid or elevate columns. They stay expand-only until the same PIT / fee / audit bar as factor-mine.

## Optional columns (not live gates)

| column | meaning | promote? |
|---|---|---|
| `avoid_veto` | Theme Radar fade: FPE≥35 or (d_RSI≥5 and d_mcap≥3%) | only if both-tape YES on the avoid scoreboard |
| `elevate_bump` | CANSLIM + clean radar + AB lead | only if both-tape YES on the elevate scoreboard |
| `radar_high_fpe` | `Forward P/E` ≥ 35 (prior Elite) | fade sticker; cheap≠long |
| `radar_cheap_fpe` | 0 < FPE ≤ 15 | **not** an elevate |
| `d_rsi` / `d_mcap_pct` | prior vs prior-prior Elite | inputs to avoid |
| `mf_flag` / `canslim_flag` | existing style flags | expand / combine, not auto-long |

Join: `Ticker` + feature export date = `feature_export_date(D)`. Script: `python -m src.finviz_style_flags --csv data/exports/finviz_{prior}.csv --prior data/exports/finviz_{prior2}.csv --ab data/ab_checklist/{prior}_ab_checklist_enriched.csv`.

## Do not

- Promote on thin-n or one-tape only.
- Treat Magic Formula cheap as a long overlay.
- Use same-day `Change` / `Gap` / RelVol as an avoid/elevate input.
- Edit `LIVE_POLICY` or `flatten_robust`.

