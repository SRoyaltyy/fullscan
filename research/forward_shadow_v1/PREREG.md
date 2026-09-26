# Forward shadow v1 — preregistration

- study: `forward_shadow_v1`
- status: protocol locked. This commit has no filled session.
- written: 2026-09-26
- forward start: 2026-09-28. No earlier session is filled.
- fill model: `keep-held`
- fingerprint_sha256: 7e845d18f1e252a2dafc88a0c4c9b22d367aaf13273035af63071bf0dc0288e7
- fingerprint_scope: SHA-256 of the UTF-8 bytes after the line `<!-- BEGIN COVERED -->`, including the final newline. Line endings are LF.

<!-- BEGIN COVERED -->

## What this study is

This is a new research study named `forward_shadow_v1`. It does not edit Factor Mine, the live book, the paper send path, Webull, flatten_robust, Supabase, or `00_grounding/paper_flatten.json`. It does not place orders.

Two recipes are frozen under new names. Each body is the Factor Mine `build_group3_recipes()` definition scored in PR #368 (`lever_search_clean`):

| forward name | group3 source | hold | top_n | rank | forbid |
| --- | --- | ---: | ---: | --- | --- |
| `fwd_union_hot_n4_h1` | `union_hot_n4_h1` | 1 | 4 | `hot_score` | alarm |
| `fwd_union_hot_score_h3` | `union_hot_score_h3` | 3 | 8 | `hot_score` | alarm |

Side is long. Universe is union. `exit_when` is empty. `trades_at_open` is true. The committed copy is `research/forward_shadow_v1/recipes.json`. Its `spec_sha256` covers that file's body, including `fill_model`.

## Fill model

`fill_model` is `keep-held`. `sell` is `list`. Min-hold is the recipe hold, counted in locked sessions. The entry session counts as zero.

A name that is already held and is still selected that morning stays. The book records no sell and no buy for that name and charges no fee. `lot_should_sell` returns keep while the name is still listed.

A sell is a list-drop: the name is off the morning list and the held-session count has reached min-hold. An `exit_when` condition on the recipe can sell earlier. These recipes have an empty `exit_when`.

When the frozen send file carries `s` and `s` is at or below `-3`, new buys sit. A missing `s` does not sit. List-drop sells still happen. A kept lot is not liquidated.

Each recipe is its own $10,000 sleeve. Shares are whole shares. Leftover cash is split equally across new names. The fill is the 09:30 open. The close is the mark. Fees are the pinned Futubull schedule, sha256 `019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3`. The flat 15 bp series reprices those same share counts at 7.5 bp per side.

## Inputs and bars

From 2026-09-28, each locked session reads `data/factor_mine/send_inputs/<date>.json` at the earliest commit of that path. A later rewrite of the file is not read. The morning picks are `pick_day` on those rows. If the frozen file has no same-day rows, the sleeve sits. If the file is not committed yet, the morning run waits and writes nothing.

No session before 2026-09-28 is written. A later run does not fill that gap.

Bars are Yahoo daily bars with `auto_adjust` false. Open, high, low, and close are split-adjusted. Dividends are not applied. Each locked session pins `research/forward_shadow_v1/bars/<date>.json` once. A consecutive stored jump above 3x or below 1/3 must be explained by a Yahoo split between those sessions. Otherwise the run fails and the day is not locked.

## Baselines

IWM is bought at the first locked session's open with whole shares and the Futubull fee, then marked at each close.

RANDOM4 is 1,000 draws. Draw `k` uses `random.Random(20260813 + k)`, samples 4 names from that day's frozen universe, and is long with hold 1. It exits at the next session open. That baseline is a time exit. The strategy book stays keep-held.

The running report is `research/forward_shadow_v1/ledger/REPORT.md`. For each sleeve it shows W/T days, closed trades, trade win rate, Futubull compound, flat 15 bp compound, compound without the best ticker, and the same sessions against IWM and the RANDOM4 median.

## v4 hook

`research/forward_shadow_v1/hooks/v4_winners.json` starts with an empty `winners` list. A later frozen winner is appended there after its fingerprint commit. `first_session` is the first locked session after that commit. Sessions already in the ledger stay as written. The winner is absent from those sessions.

## Append-only

`research/forward_shadow_v1/ledger/manifest.jsonl` is empty in this commit. Each new picks file and fills file is one line. A later change may only add a session after the last recorded one of that kind. `.github/workflows/forward_shadow_v1_append_only.yml` fails the pull request when an earlier line, picks file, fills file, or bar file changes, disappears, or is reordered, when the recipe spec or this preregistration changes, or when a v4 winner is edited, removed, or given a first session on or before a session already in the base ledger.

`.github/workflows/forward_shadow_v1.yml` runs on weekdays after the morning ticket publish and again after the close. It commits only `research/forward_shadow_v1/`.
