# Price version (a) — drop-list citation

This note does not rescore. `PREREG.md`, `INPUTS.json`, `DROP_LIST.json`, `freeze/FREEZE.json`, and `returns/REPORT.md` are unchanged. The clean tape already walked is the #362 parquet below, so the compounds, the freeze order, and the benchmarks stay as published.

Version (a), `clean`, is `research/breadth_rank_v1c/bars/ohlc.parquet` on PR #362 branch `cursor/breadth-rank-v1-ef2d`, commit `ad10f862ad51df88f4dd03ff3dbcdf628f7e2685`.

- parquet sha256: `5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2`
- parquet blob: `e7bbac335cfa87243f9d0ddf8c331a0739dd0df8`

The drop list is `research/breadth_rank_v1c/bars/DROPPED.json` at that same commit. Resolutions are `research/breadth_rank_v1c/JUMPS.md`.

- DROPPED.json sha256: `4da67a52e469be8ccd1430b5b7992a1d70b7542bcca67851d7d6182c267ce6ad`
- DROPPED.json blob: `31e3b7dc38583288faba338001a91503bf69a758`
- JUMPS.md sha256: `8080267a532fff2ea4c9e225fdf006f38f4ddeb5ca8e30900b16a320e58ce05a`
- JUMPS.md blob: `c24377fa4c19b18d51251bcc3e6948bbcaef2c77`

`JUMPS.md` records 93 unexplained open/close legs. Those legs are 76 tickers. That is the drop list for version (a). It is not the older 52-name open/previous-close subset.

ALP, NFE, TNMG, and WCT are inside those 76. Each matched a real Yahoo split. The re-pull still jumps, so they were not rescaled. They are flagged here and dropped on version (a).

`DROPPED.json` also lists YAAS. `JUMPS.md` puts YAAS after the 93 and says it is not one of them. The parquet omits YAAS as well as the 76. This study's copied `DROP_LIST.json` is that 77-name file, same sha as above.

Version (b), `yahoo`, is `data/prices/ohlc.parquet`, sha256 `559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55`. It keeps the 76 tickers and YAAS. The four splits stay flagged and are not removed from that book.

On the frozen top 20, at X=2, 4, and 8, attributed Futubull dollars:

| window | version | 76 tickers | four splits | YAAS |
| --- | --- | ---: | ---: | ---: |
| through 2026-09-11 | clean | 0.00 / -21772.32 (0.00%) | 0.00 | 0.00 |
| through 2026-09-11 | yahoo | -3498.93 / -23953.15 (14.61%) | 0.00 | 0.00 |
| 2026-09-14 through 2026-09-25 | clean | 0.00 / -46166.81 (0.00%) | 0.00 | 0.00 |
| 2026-09-14 through 2026-09-25 | yahoo | -45.97 / -44884.59 (0.10%) | 0.00 | 0.00 |

The 76-ticker share equals the 77-name share already printed in `returns/REPORT.md`, because YAAS is $0 in these books. The four flagged splits are $0 in these books. None of the 76 is present on the cleaned tape.
