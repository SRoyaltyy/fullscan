# Factor-mine aux morning panel feeds

Research / ops note. Does not change live `flatten_robust` or Webull money
paths. The 09:30 shopping list is a paper wish-list.

## What broke (2026-09-16 onward)

`data/factor_mine/panel.json` through 2026-09-15 unions clock-clean sources
(`flatten` + `ohlc_hot` + `probable` + `yday_gainer` + `yday_mover` +
`earn_react` + often `mover_buy`), n≈60–100 names/day.

From **2026-09-16** the committed panel is flatten-only (n=4–6). Aux list
builders did not fail because Finviz files went empty:

| prior session | `finviz_YYYY-MM-DD.csv` | usable Change/Volume |
|---|---|---|
| 2026-09-15 | present, ~11.6k rows | yes |
| 2026-09-16 | present, ~11.6k rows | yes |
| 2026-09-17 | present, ~11.6k rows | yes |

The code path did. Scheduled / workflow-run factor-mine uses `--land-closed`.
Morning Pre-Open / an explicit open `--to-date` calls
`extend_pack_through` → `build_panel(today, today)`. That walk used the
**emit** calendar `[today]` as the lookback calendar.

`gainer_capture.prior_session([D], D)` is `None`. Every Finviz / OHLC feed
(`yday_gainer`, `yday_mover`, `ohlc_hot`, `probable`, `earn_react`) then
returns `[]`. Flatten still lands because `flatten_day_targets` loads the
full stock-book calendar itself. `mover_buy` is independent of Finviz but
needs a priced 09:30 open — morning extend often has none, so the day
prints flatten-only.

After close, `payload_covers_session` was already true (the flatten-only
day was on the board), so land-closed **skipped** and never rebuilt aux.
The starved rows stayed in `panel.json`. 2026-08-27 is the same shape
(flatten + mover_buy only): one-day emit, plus `finviz_2026-08-26.csv`
is missing.

Same-day Finviz is still a leak and is never used as a feature.

## What the fix does

1. Aux builders take `lookback_calendar` = emit window ∪ stock-book
   session calendar, so a one-day land-closed walk still sees yesterday.
2. `knowable_export_date` walks back to the last **readable** Finviz
   export strictly before the session (skips the missing 08-26 file).
3. `land_closed` does not skip when the cached panel is flatten-only.
4. `load_or_build_panel` repairs starved days in place (does not rescan
   healthy 08-13→09-15 rows) and stamps `lookback: full_session_cal`.

## Remine after merge (prove `panel_n` recovers)

On `main`, after this PR is merged:

```bash
# Preferred: reuse existing recipes; repair flatten-only days; restamp books.
python -m src.factor_mine --from-date 2026-08-13 --write --land-closed
```

Nuclear (rebuild every panel day, then remine):

```bash
python -m src.factor_mine --from-date 2026-08-13 --write --land-closed --rebuild-panel
```

GitHub Actions equivalent: workflow `factor_mine.yml` dispatch with
`land_closed=true`. Add `rebuild_panel=true` only for the nuclear path.

### Verify

```bash
python - <<'PY'
import json
from collections import Counter, defaultdict
p = json.loads(open("data/factor_mine/panel.json", encoding="utf-8").read())
by = defaultdict(list)
for r in p["rows"]:
    by[r["date"]].append(r)
print("to", p["to_date"], "n_rows", p["n_rows"], "lookback", p.get("lookback"))
for d in p["session_dates"]:
    srcs = Counter()
    for r in by[d]:
        srcs.update(r.get("sources") or [])
    print(d, "n=", len(by[d]), dict(srcs))
PY
```

Expect:

- `lookback` is `full_session_cal`
- `n_rows` well above the starved 1790 (about +60–100 names per repaired
  day; 09-16/17/18 plus 08-27)
- 2026-09-16 and later show `yday_gainer` / `ohlc_hot` / `probable` (and
  usually `yday_mover`); flatten-only on those days means the remine did
  not run or Finviz for the **prior** session is still unreadable
- 2026-08-13 may stay flatten-only (no prior tape by construction)

Then confirm cash-start / union recipes on 09-16+ are no longer choosing
from 4–6 flatten names only.

Do **not** retune gates or rankers until this remine shows `panel_n`
recovered. Live Webull / `flatten_robust` stay frozen.

Clock-B / 09:30-knowable catalogue wiring (have / calculable tells already
on this panel, not KEEP) is documented in [CLOCK_B_TELLS.md](CLOCK_B_TELLS.md).
Theme Radar T−1 gap+RelVol is an optional stamp/filter (or
`FULLSCAN_OPPSET_UNION=1` remine source) — pull the CSV over HTTPS, no
clone. Same-day Gap / RelVol stay leaks.
