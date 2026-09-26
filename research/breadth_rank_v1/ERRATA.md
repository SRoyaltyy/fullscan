# ERRATA — breadth_rank_v1

This file records a known limit. It does not edit `research/breadth_rank_v1/bars/ohlc.parquet`, `FROZEN_RANK.json`, `PREREG.md`, or any file under `research/breadth_rank_v1/returns/`.

`breadth_mine_v1b` pinned a Yahoo snapshot with 57 consecutive-session open/previous-close jumps that no Yahoo split on the later date explains. `breadth_rank_v1` had already dropped every ticker that jumped in its own pull, including names whose jump is a real split the adjusted Close still contains. Those prices were not rescaled, and the old snapshot is not replaced.

The resolution of each of those 57 jumps, and the score on the cleaned snapshot, are the new study `breadth_rank_v1b`. IRONCLAD 25: a price-data change and a new selection metric are a new name. This study's 40 tries stay in the luck tally.
