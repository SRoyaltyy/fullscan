# Clock-B catalogue tells (factor-mine)

Research / ops note. Does **not** change live `flatten_robust` or Webull
money paths. Does **not** claim a Cyrus KEEP edge. Excel fee-KEEP prove
of the 10 priority combos is a separate job. Theme Radar T−1 gap + RelVol
is an optional panel/universe feed (stamp/filter, or remine union).

Clock-B = 09:30-knowable. No VWAP / intraday for open fills. Same-day
Change% / Gap / RelVol stay leaks, never gates.

## James map (have / calculable / need-source)

Cyrus Stock Direction Tell Catalogue: 24 families / 240 entries —
candidate predictors, not proven edges. Fullscan already computed most
of the Clock-B atoms as panel columns or morning cameras; they were
not in the morning recipe union. This PR surfaces those columns into
named recipe gates.

| # | Family | Status | Fullscan columns (already computed) |
|---|---|---|---|
| 1 | Moderate / tape momentum | have | `ohlc_ret_5`, `macd_up`, `last_green`, `ohlc_hot_score` |
| 2 | Completed breakout | have | `ohlc_break_10`, `candle_capture` |
| 3 | Peer RS | have | `boxes.peer`, scan `rs_week` |
| 4 | Sector RS | have | `boxes.sector` |
| 5 | Positive catalyst | have | `boxes.catal`, `e_pol`, `news_*`, `erd_earn_react` |
| 6 | Limited / extreme extension | have | `ohlc_ret_5`, `ohlc_rvol`, `rsi_ob` |
| 7 | Earnings improvement | have | `erd_flag_E`, `e_pol`, `erd_days_since_E` |
| 8 | Raised guidance | calculable | `news_prior` / `news_box` (NEWS_POS includes `raises`) |
| 9 | Favorable earnings reaction | have | `erd_earn_react`, `e_pol` |
| 10 | Negative catalyst | have | `news_box`, `news_prior`, `e_pol` |
| 11 | Relative weakness | have | `boxes.peer` / `sector`, `ohlc_ret_5` |
| 12 | Failed recovery | have | `last_red`, `macd_down` |
| 13 | Diminishing progress | calculable | `ohlc_ret_5` vs `ohlc_ret_10` |
| 14 | Failed breakout | calculable | `ohlc_break_10` and `last_red` |
| 15 | Holds vs sector weakens | calculable | `ohlc_ret_1` / `last_green` + `boxes.sector` bad |
| 16 | Insider buying | calculable | headline `ins_buy` + Form-4 monthly panel if present |
| 17 | Improving cash economics | calculable | prior-export `fv_inst` > 0 (not true FCF) |
| 18 | Stabilization | have | `flow_in`, `rsi_os`, coil |
| 19 | Prior RelVol | have | `ohlc_rvol`, `fv_rvol` |
| 20 | Analyst revision | have | `erd_flag_R`, `erd_days_since_R` |
| 21 | Tape flow | have | `flow_in`, `macd_up` |
| 22 | Compression / NR7 | have | `ohlc_nr7` |
| 23 | Morning weather S | have | book-level sit; already `combo_se_5050_weather` |
| 24 | News tone | have | `news_box`, `news_prior` |

**Need-source (not wired):** VWAP / intraday open fills; same-day Gap /
RelVol from snapshot T (leak); Webull live; structured guidance events;
true FCF statements; same-day E stamped after 09:30 (`finviz_events`
already drops those). Theme Radar **T−1** gap+RelVol is Clock-B and is
wired as an optional feed — see below.

## 10 priority combos wired

| # | Recipe | Gate | Side |
|---|---|---|---|
| 1 | `union_clk_mom_break_peer_h1` | `clk_mom_break_peer` | long |
| 2 | `union_clk_fresh_cat_coil_h1` | `clk_fresh_cat_coil` | long |
| 3 | `union_clk_earn_guide_react_h1` | `clk_earn_guide_react` | long (knowable E only) |
| 4 | `short_clk_neg_weak_fail_h3` | `clk_neg_weak_fail` | short |
| 5 | `short_clk_ext_veto_h3` + forbid on longs | `clk_ext_veto` | veto / short |
| 6 | `union_clk_hold_vs_sector_h1` | `clk_hold_vs_sector` | long |
| 7 | `union_clk_insider_cash_stab_h3` | `clk_insider_cash_stab` | long (if insider/Form-4 present) |
| 8 | `union_clk_flow_coil_h1` | `clk_flow_coil` | long |
| 9 | `union_clk_r_up_coil_h1` | `clk_r_up_coil` | long |
| 10 | `union_clk_nr7_mom_h1` | `clk_nr7_mom` | long |

Ship-now splice is combos **1 / 2 / 4 / 5 / 6 / 10** plus the Theme Radar
oppset hook (`CLOCK_B_CORE` + `CLOCK_B_OPPSET_RECIPES`). Combos 3 / 7 / 8 / 9
stay wired and testable; they are not on `--splice-clock-b`.

Long Clock-B sleeves also `forbid` `#5` (`clk_ext_veto`) and `alarm`.
Collectors used: `factor_mine` panel union, `ohlc_ripper`, `candle_factor`,
`peer_rs` (via `boxes.peer` / `rs_week`), `finviz_events`, prior Finviz
export (`fv_inst`), optional `data/insider/history/monthly_panel.csv`,
headline tone. Weather S stays book-level.

## Clock-B proof

- Every atom is prior tape, morning packet, or a Finviz export strictly
  before session D. Same-day Change% / Gap / RelVol / VWAP are in
  `clock_b_tells.LEAK_FIELDS` and are **absent** from `INPUT_FIELDS`.
- Combo 3 refuses `e_pol == bad` and `days_since_E > 5`. Same-day E after
  09:30 never enters `finviz_events.asof_snapshot`.
- Form-4 uses the last **completed month** `< D[:7]`.
- Fills remain 09:30 open. No Webull path change.

Unit proof: `python -m src.test_clock_b_tells` and `python -m src.test_factor_mine`.

## Remine / smoke (after #277)

#277 restored aux morning feeds. Remine `35438833792` proved multi-src
again: **09-16 n=69, 09-17 n=66, 09-18 n=58** (`flatten` + `ohlc_hot` +
`probable` + `yday_gainer` + `yday_mover` + often `earn_react`).
`lookback: full_session_cal`. See [FACTOR_MINE_AUX_PANEL.md](FACTOR_MINE_AUX_PANEL.md).

This PR evaluates Clock-B gates **on that restored panel**. It does not
rebuild `factor_mine.json` and does not retune the 24 workable Pages
recipes.

Smoke fires on 09-16/17/18 after `attach_erd_polarity` (knowable `e_pol`).
Not KEEP — fire count only:

| Combo | 09-16 | 09-17 | 09-18 | notes |
|---|---|---|---|---|
| #1 mom+break+peer | 17 | 15 | 12 | columns were already on the panel |
| #2 fresh cat + coil | 24 | 26 | 16 | lifts once `e_pol` is attached |
| #3 E+guide+react | 0 | 0 | 0 | wired; no 09:30-knowable triple hit those days |
| #4 neg+weak+fail | 12 | 8 | 2 | short |
| #5 ext veto | 4 | 5 | 5 | long forbid + short sleeve |
| #6 hold vs sector | 19 | 18 | 9 | `boxes.sector` already computed |
| #7 insider+cash+stab | 0 | 0 | 0 | 1 Form-4 name; no insider+stabilize intersection |
| #8 flow coil | 1 | 0 | 0 | thin but present |
| #9 upgrade + coil | 0 | 0 | 0 | wired; no knowable R-up ∩ coil those days |
| #10 NR7 + mom | 1 | 4 | 0 | |

Smoke (no write, no KEEP):

```bash
python -m src.test_clock_b_tells
python -m src.test_factor_mine
python - <<'PY'
import json
from src import clock_b_tells as cbt
p = json.loads(open("data/factor_mine/panel.json", encoding="utf-8").read())
print(cbt.panel_fire_counts(p, ["2026-09-16","2026-09-17","2026-09-18"]))
PY
```

Optional later cash-book splice (research only; do not read as KEEP):

```bash
python -m src.factor_mine --splice-clock-b
# or a gate slice: python -m src.factor_mine --gate clk_b --no-auto-tweak --no-combo
```

Do **not** merge until checks are green. Do **not** claim KEEP.

## Theme Radar Clock-B opportunity-set (optional feed)

Live on [SRoyaltyy/theme-radar `main` @ `a782cc2b`](https://github.com/SRoyaltyy/theme-radar/tree/main/research/oppset_clock_b):
24 join mornings, ~53k clean, ~7.4k flagged. Features = T−1 only
(`join_morning` = decision T, `finviz_asof` = T−1). Same-day Gap /
RelVol / Change from snapshot T are outcomes and are **never** joined
(`parse_rows` drops `finviz_asof >= join_morning`).

Proof on the live flagged CSV:

| `join_morning` | flagged n | notes |
|---|---|---|
| 2026-09-16 | 242 | |
| 2026-09-17 | 261 | |
| 2026-09-18 | 451 | SDGR / GNRC `finviz_asof` 2026-09-17 |

Research-only. No live Webull. Do not commit the CSV.

### Pull / join without cloning theme-radar

Pinned raw URL (HTTPS only — no `git clone`):

```
https://raw.githubusercontent.com/SRoyaltyy/theme-radar/a782cc2b/research/oppset_clock_b/oppset_flagged.csv
```

```bash
# one-shot cache (gitignored)
python -m src.factor_mine --pull-oppset

# equivalent curl
mkdir -p data/factor_mine/oppset_clock_b
curl -fsSL \
  https://raw.githubusercontent.com/SRoyaltyy/theme-radar/a782cc2b/research/oppset_clock_b/oppset_flagged.csv \
  -o data/factor_mine/oppset_clock_b/oppset_flagged.csv
```

Override path with `FULLSCAN_OPPSET_CSV=/path/to/oppset_flagged.csv`.
`discover_csv()` also accepts `/tmp/oppset_clock_b/oppset_flagged.csv`
or a sibling `../theme-radar/research/oppset_clock_b/oppset_flagged.csv`
checkout if one already exists.

Join key is `(join_morning, ticker)` → panel `(date, ticker)`. Stamped
fields (never aliases of same-day Change / Gap / RelVol):

- `oppset` / `opp_any`
- `opp_rvol` / `opp_gap_pct` / `opp_change_pct`
- `opp_finviz_asof`

Default mode is **stamp + rank/filter** on the existing union so
`--land-closed` does not add 242–451 names/day. Opt-in remine union
(top 30 by T−1 rvol as a panel source):

```bash
FULLSCAN_OPPSET_UNION=1 python -m src.factor_mine --rebuild-panel --write
```

GitHub Actions equivalent (`factor_mine.yml` workflow_dispatch). Default
`oppset_union` is **false** so scheduled / `workflow_run` land-closed
stays stamp-only:

- `rebuild_panel=true`
- `oppset_union=true` (sets `FULLSCAN_OPPSET_UNION=1` and runs `--pull-oppset`)
- `from_date=2026-08-13`

That caches `data/factor_mine/oppset_clock_b/oppset_flagged.csv`
(gitignored) and rebuilds the panel with top-30 T−1 flagged names as an
`oppset` source. Do not enable on nightly land-closed.

Recipes (research; not KEEP):

| Recipe | What it does |
|---|---|
| `union_oppset_h1` | union ∩ stamped `oppset`, rank `opp_rvol` |
| `oppset_h1` | dedicated `oppset` universe, rank `opp_rvol` |
| `union_clk_*_opp_h1` / `short_clk_*_opp_h3` | combos 1 / 2 / 4 / 5 / 6 / 10 ∩ oppset |

```bash
python -m src.test_oppset_clock_b
python -m src.factor_mine --splice-clock-b   # CORE 1/2/4/5/6/10 + oppset hook
# or: python -m src.factor_mine --gate oppset --no-auto-tweak --no-combo
```
