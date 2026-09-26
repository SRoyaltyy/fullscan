# Deviations

The tests in `PREREG.md` and `ADDENDUM_1.md` are unchanged. This file records storage only.

## Bar cache is sharded

`PREREG.md` names `research/longhist/bars/ohlcv.parquet` and `research/longhist/bars/manifest.jsonl`. The cache has 9,959,662 bars. One manifest of those hashes is about 1.1GB, and one parquet is about 154MB. GitHub rejects a file over 100MB.

The same bytes are stored as ordered shards:

- `research/longhist/bars/ohlcv/part-NNNNN.parquet`
- `research/longhist/bars/manifest/part-NNNNN.jsonl`

Read parts in name order. That concatenation is the append-only cache. A later bar is a new part. No shard is rewritten. `research/longhist/bars/CACHE_INDEX.json` has each shard's sha256, byte size, and row count.

Each manifest line is `{"date","sha256","ticker"}` as specified. The sha256 is of the canonical 6-decimal bar JSON defined in the preregistration. Five thousand bars in the first shard were re-hashed from the parquet and matched.

## Symbols with no bar

Two Yahoo responses had no usable quote bar on or before 2026-08-12 and are not in the cache: `ADRX`, `HLSQ`.

Twelve symbols returned HTTP errors on the fetch and the retry, so they have no bars: `ADBT`, `ASBH`, `AUXX`, `ETRA`, `FBDT`, `HOST`, `LYNX`, `OIG`, `SGLD`, `SWRD`, `TREO`, `XTND`.
