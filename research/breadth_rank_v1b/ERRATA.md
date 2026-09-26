# ERRATA — breadth_rank_v1b

This file does not edit the pinned bars, `FROZEN_RANK.json`, `PREREG.md`, or any file under `research/breadth_rank_v1b/returns/`.

The jump gate flags both `open/previous-close` and `close/open`. The 57 open/previous-close legs were already dropped. Thirty-six more legs are `close/open` and are also unexplained after a Yahoo re-pull. Eight of those names are still inside this study's pinned snapshot: ADBT, FIRY, JLHL, LGCL, NXTT, SMJF, XHLD, YXT. Removing them changes the prices, so IRONCLAD 25 puts that snapshot in `breadth_rank_v1c`. This study's 40 tries stay in the luck tally.
