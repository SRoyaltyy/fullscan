# Theme Radar extreme flags vs morning sleeves

_Research only · live `flatten_robust` untouched · no rubric-weight change._

Flags: Theme Radar `ext == extreme` (week ≥ 100% **or** +40% vs 50-DMA). Join key **`join_morning`** = next weekday session after the after-close flag date.

Provenance: Theme Radar already-built `data/universe/*_membership.csv` (`ext == extreme`) + snapshots, written to `/workspace/finviz-abnormal-volprice/` then joined on `join_morning`. Shared-box CSVs were not pre-mounted on this VM. Not a fullscan Finviz rescreen.

Denom (`extreme_flags_full.csv`): **220739** name-days. Extreme: **2183** name-days / **404** tickers / **19** join mornings. ETF: 638. Hot RelVol bin: 727.

This is **not** the first-pass RelVol≥3 / |day|≥8% / week≥+40% rebuild. Theme Radar's extreme bucket is louder on price (week ≥ 100% or SMA50 ≥ +40%) and does **not** require a +8% day.

## Plain board

Each row: how many extremes that sleeve could have taken, how many it actually took, why the rest died, and a thin after-fee force-include (T+1 Change-from-Open − 15 bp, equal-weight, not a cash book).

| Sleeve | Extreme n | Eligible | Chosen | Top blockers | Ext-only Hff | Current Hff | Force-in Hff |
|---|---:|---:|---:|---|---:|---:|---:|
| Factor-mine 09:30 panel / recipes | 2183 | 161 | 58 | not_in_factor_mine_panel 2022, chosen 58, in_panel_not_recipe_pick 58, too_extended_ret5_or_rvol 45 | +0.16 n=2029 | -0.05 (18d) | +0.18 (18d) |
| Flatten wish-list / live GO | 2183 | 589 | 4 | not_in_morning_book 956, etf 638, flatten_gate_off 297, HARD_RED_sit 203 | +0.16 n=2029 | +0.43 (18d) | +0.23 (18d) |
| Green pile | 2183 | 22 | 16 | not_in_morning_book 1570, not_green_cores 336, illiquid_mcap_lt_400_or_micro 242, green_pile_unused 19 | +0.16 n=2029 | +0.64 (5d) | -0.01 (18d) |
| Live sleeve_merge tickets | 2183 | 168 | 5 | not_in_morning_book 1570, illiquid_mcap_lt_400_or_micro 248, not_ticketed 213, dead_relvol_0_0.7 147 | +0.16 n=2029 | +0.62 (8d) | +0.18 (18d) |
| Excel L1 lowvol (skip loud by design) | 2183 | 22 | 0 | loud_volM_ge3_skip_by_design 1514, etf 638, cohort_ok_not_signaled 22, volM_unknown 9 | +0.16 n=2029 | +0.10 (10d) | +0.05 (18d) |
| Excel L2 lowvol (skip loud by design) | 2183 | 22 | 0 | loud_volM_ge3_skip_by_design 1514, etf 638, cohort_ok_not_signaled 22, volM_unknown 9 | +0.16 n=2029 | +0.10 (10d) | +0.05 (18d) |
| Excel L3 midcap (skip loud by design) | 2183 | 212 | 0 | not_midcap_skip_loud_by_design 1333, etf 638, cohort_ok_not_signaled 212 | +0.16 n=2029 | +0.26 (10d) | +0.12 (18d) |
| Excel L4 BBAI-like | 2183 | 59 | 0 | not_midcap 1333, etf 638, not_hibeta 147, cohort_ok_not_signaled 59 | +0.16 n=2029 | — (0d) | +0.21 (18d) |
| Excel L5 midhibeta (loud-tolerant) | 2183 | 65 | 0 | not_midcap 1333, etf 638, not_hibeta 147, cohort_ok_not_signaled 65 | +0.16 n=2029 | +0.20 (8d) | +0.21 (18d) |

### How to read eligible / chosen

- **Factor-mine eligible** = ticker is on the 09:30 panel (flatten / probable / yday_gainer / ohlc_hot / earn / mover). **Chosen** = landed in a recipe top-8 (`union` / `yday_gainer` / `flatten` / `ohlc_hot` / `probable`).
- **Flatten eligible** = printed in the T+1 morning book (wish-list can still name micros). **Chosen** = flatten would-buy / wish-list. Live tickets still need flatten GO and no HARD_RED.
- **Green eligible** = morning book + cores green + not dead RelVol + $400M BUY floor. **Chosen** = on that morning's used pile.
- **Sleeve eligible** = BUY-walk seat possible (book + $400M + not dead RelVol + lattice). **Chosen** = actually ticketed in `data/sleeve_merge/trades.csv` on `join_morning`.
- **Excel L1/L2** need `Volatility (Month) < 3%`. Exploded week names fail. Skip loud by design.
- **Excel L3** needs Theme Radar `size == mid` ($2–10B). Most extremes are micro/small. Skip loud by design.
- **Excel L4** mid + beta>1.5 + unprofitable (BBAI-like). **L5** mid + beta>1.5 (loud-tolerant).
- Excel **chosen** = `suggestions.csv` signal confirmed on flag date (buy next open = `join_morning`), or stamped the same morning.

### Chosen extremes

- **Factor-mine 09:30 panel / recipes** (58): AIRO 2026-08-13→2026-08-14, ARX 2026-08-13→2026-08-14, BETA 2026-08-13→2026-08-14, LIFE 2026-08-13→2026-08-14, OMER 2026-08-13→2026-08-14, HTFL 2026-08-14→2026-08-17, IOVA 2026-08-14→2026-08-17, UMAC 2026-08-14→2026-08-17
- **Flatten wish-list / live GO** (4): ARCT 2026-08-20→2026-08-21, AUTL 2026-08-20→2026-08-21, CRDL 2026-08-20→2026-08-21, CYPH 2026-08-20→2026-08-21
- **Green pile** (16): CRDL 2026-09-01→2026-09-02, ALEC 2026-09-02→2026-09-03, ARCT 2026-09-02→2026-09-03, BMEA 2026-09-02→2026-09-03, EMBC 2026-09-02→2026-09-03, IOVA 2026-09-02→2026-09-03, OABI 2026-09-02→2026-09-03, SENS 2026-09-02→2026-09-03
- **Live sleeve_merge tickets** (5): ARCT 2026-08-20→2026-08-21, AUTL 2026-08-20→2026-08-21, CRDL 2026-08-20→2026-08-21, CYPH 2026-08-20→2026-08-21, OABI 2026-09-03→2026-09-04

## Thin after-fee counterfactual

Equal-weight T+1 **Change from Open** minus **15 bp**. Not dollar-weighted. Current = that sleeve's actual morning picks. Force-include = those picks **plus** that morning's extremes. Extremes-only is the same book for every sleeve (2183 name-days).

Extremes-only name-day after-fee H: **+0.16** (n=2029, H+ 40.5%).
Hot-RelVol extremes only (abnormal vol × extreme price): **+0.66** n=674 H+ 40.7% (n_flags=727).

Force-include **drags** flatten (+0.43 → +0.23), green (+0.64 → −0.01), and sleeve (+0.62 → +0.18). It **lifts** factor-mine (−0.05 → +0.18). Excel L1–L5 current books are quiet names; dumping 2k extremes into them is a miss-check, not a trade.

## Flatten / sleeve tickets that did print

| Sleeve | Flag T | join_morning | Ticker | Week | RelVol | mcap $M | Hff |
|---|---|---|---|---:|---:|---:|---:|
| flatten+sleeve | 2026-08-20 | 2026-08-21 | **ARCT** | +39.8 | +3.98 | 376.73 | +19.24 |
| flatten+sleeve | 2026-08-20 | 2026-08-21 | **AUTL** | +10.8 | +1.75 | 645.44 | -2.76 |
| flatten+sleeve | 2026-08-20 | 2026-08-21 | **CRDL** | +10.2 | +1.99 | 220.21 | -2.48 |
| flatten+sleeve | 2026-08-20 | 2026-08-21 | **CYPH** | +83.6 | +2.51 | 138.98 | -3.27 |
| sleeve | 2026-09-03 | 2026-09-04 | **OABI** | +3.0 | +1.73 | 690.61 | -6.65 |

08-21 is a flatten **GO** morning (not HARD_RED). ARCT continued (+19.24). AUTL / CRDL / CYPH faded. OABI is a 09-04 sleeve ticket (−6.65).

## Hot RelVol subset (abnormal volume)

Theme Radar `rvol == hot` (≥ 1.5×): **727 / 2183** extremes. Every flatten and sleeve ticket above is in this subset. Factor-mine recipe picks: 34. Green pile: 8. Excel L1–L5 still **0**.

## Excel L1–L3 skip loud

L1/L2 are `volM:low(<3%)`. A Theme Radar extreme (week ≥ 100% or +40% vs 50-DMA) almost never prints a <3% month vol — **~97% fail the cohort before a cluster is even scored**. L3 is mid-cap only — most extremes are micro/small. L5 is the Excel sleeve that *can* hold a loud mid hibeta name, and it still did not signal one. Zero Excel L1–L5 suggestions overlap these flags.

## Gaps

- Shared-box CSVs were **not mounted** on this VM (`/workspace/finviz-abnormal-volprice` was empty). Flags were read from Theme Radar's already-built `data/universe/*_membership.csv` (`ext == extreme`) plus snapshots. Not a fullscan Finviz rescreen.
- Theme Radar has no `2026-08-27` membership file (no snapshot that day).
- Missing T+1 stock books (cannot prove BUY-walk): 08-24, 08-25, 08-26, 08-28.
- Flag date 09-11 has no in-window `join_morning` and is dropped.
- Green **chosen** can include micros on a used pile; the $400M eligible cut is stricter (22 eligible, 16 on-pile, 3 of those 16 clear the BUY floor).
- If another Theme Radar agent later drops a tighter vol/price `extreme_flags.csv` on the shared box, rerun this script — mounted CSVs win.

Live wire not touched.

