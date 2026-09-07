# First A–JL cell/formula cut

_Generated 2026-09-07 · live `flatten_robust` is not changed. No merge without Cyrus._

## Protocol

- Clocks from `CLOCK_MAP.md` / `clock_map.json` (timing_test + formula deps). Unknown → **close**. `core_score` = **CLOSE**.
- Sleeve-native holds **1 / 2 / 3**. Cost = **futubull** 0.15% long / 0.20% short (labeled on every row).
- Lottery: one trade ≤25% of gross wins; drop-best mean >0.
- PASS needs ship bar **and** both tape halves avg>0 (n≥40 each).

Sample **185** tickers. Patterns **946**. Cells scored **1289**. **PASS 0** · **FAIL 1277** · **THIN 12**.

## Clean null

No cell/formula cleared n + effect + both-tape + lottery + hold1-sibling on this 185-ticker A–JL sample. That is the honest first-cut result, not a keep.

A prior pass listed 20 open-entry PASSes on unmeasured fills (GU/GW/HC/GY/HD/CP). Those were demoted: only A–O fills have a `timing_test` license to enter at open. At close they do not clear the bar (the same-day color *is* the move).

## Near (holdout t≥2, disc avg>0, still FAIL)

| def | clock | side | exit | disc | hold | early | late | why |
|---|---|---|---|---|---|---|---|---|
| `BE_ge1` | close | long | hold1 | 108/+1.48%/t=3.9 | 66/+1.85%/t=4.9 | 174/+1.62%/t=5.9 | — | thin_disc,thin_hold,tape_thin |
| `AO_le-1` | close | short | hold1 | 94/+1.68%/t=3.6 | 60/+1.69%/t=4.5 | 154/+1.68%/t=5.3 | — | thin_disc,thin_hold,tape_thin |
| `BE_ge1` | close | long | hold3 | 108/+2.22%/t=4.3 | 66/+3.06%/t=3.8 | 174/+2.54%/t=5.7 | — | thin_disc,thin_hold,tape_thin |
| `BE_ge1` | close | long | hold2 | 108/+1.32%/t=3.1 | 66/+2.14%/t=3.5 | 174/+1.63%/t=4.6 | — | thin_disc,thin_hold,tape_thin |
| `CN_ge1` | close | long | hold3 | 12570/+0.21%/t=2.8 | 7819/+0.21%/t=3.0 | 5720/+0.80%/t=8.0 | 14669/-0.02%/t=-0.3 | disc_t,tape_split |
| `BO_ge1` | close | long | hold1 | 114/+1.29%/t=2.7 | 71/+1.25%/t=2.9 | — | 185/+1.28%/t=3.8 | thin_disc,thin_hold,disc_t,tape_thin |
| `AW_fill_red` | close | short | hold2 | 1074/+0.01%/t=0.0 | 652/+0.84%/t=2.8 | 1726/+0.32%/t=1.4 | — | disc_t,lottery,tape_thin |
| `Q_ge1` | open | long | hold1 | 258/+0.20%/t=0.5 | 167/+0.86%/t=2.4 | 240/+0.62%/t=1.3 | 185/+0.25%/t=1.1 | thin_disc,disc_t |
| `AW_fill_red` | close | short | hold1 | 1074/+0.11%/t=0.4 | 652/+0.44%/t=2.1 | 1726/+0.23%/t=1.4 | — | disc_t,tape_thin |

Research only. No cards. No live wire.

