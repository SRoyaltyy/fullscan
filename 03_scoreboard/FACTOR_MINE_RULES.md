# Factor Mine rule lock

Each recipe's gates, weights, hold, take-profit and exit, sizing, and fee model are hashed into `data/factor_mine/freeze_manifest.json` the first time the name is locked. The same name with a different hash fails the job. It does not rescore. Changing the rules means a new name.

Same-bar exit, written into every fingerprint: when one daily bar touches both take-profit and stop, assume **stop_first**.

Trades that rule can change: **0**. Recipes with neither a take-profit nor a stop are 0. Recipes with a stop and no take-profit cannot double-touch, so they are 0 too: `union_white_both_n4_h5_s12`, `flatten_h5_s8`. This count is the definition, not a rescore.

A job that would change picks, fills, or P&L on a locked day fails. The first backfill of a recipe with no locked history is the exception, and those pre-creation days stay `designed_after`.

See `03_scoreboard/FACTOR_MINE_DESIGNED_AFTER.md` for the split totals.

`data/day_board/<date>_strategy_tickets.json` stays the send-time file. After that session's 09:30 ET, a different body does not replace it.
