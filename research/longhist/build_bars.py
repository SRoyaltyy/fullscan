"""Turn cached Yahoo chart JSON into the append-only bar cache.

Signals later read the quote OHLC (not adjusted close). Each bar is
hashed as canonical JSON with prices rounded half-away-from-zero to
6 decimal places. GitHub rejects files over 100MB, so the cache is
ordered shards. Concatenate parts in name order; never rewrite one.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
BARS = ROOT / "bars"
SRC = Path("/tmp/longhist-bars/ok")
ET = ZoneInfo("America/New_York")
LAST_DAY = "2026-08-12"
ROWS_PER_PART = 450_000

SCHEMA = pa.schema([
    ("date", pa.string()),
    ("ticker", pa.string()),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.int64()),
    ("adj_close", pa.float64()),
])


def _price(value: float) -> str:
    rounded = Decimal(str(value)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
    return format(rounded, "f")


def canonical_bar(date: str, ticker: str, open_: float, high: float,
                  low: float, close: float, volume: int) -> str:
    return (
        '{"close":%s,"date":"%s","high":%s,"low":%s,"open":%s,'
        '"ticker":"%s","volume":%d}'
        % (_price(close), date, _price(high), _price(low), _price(open_), ticker, volume)
    )


def bar_sha256(date: str, ticker: str, open_: float, high: float,
               low: float, close: float, volume: int) -> tuple[str, str]:
    text = canonical_bar(date, ticker, open_, high, low, close, volume)
    return text, hashlib.sha256(text.encode()).hexdigest()


def _ymd(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), ET).date().isoformat()


def _num(value):
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None
    return number


def parse_chart(path: Path) -> list[dict]:
    raw = json.loads(path.read_text())
    result = (raw.get("chart") or {}).get("result") or []
    if not result:
        return []
    block = result[0]
    meta = block.get("meta") or {}
    ticker = str(meta.get("symbol") or path.stem)
    stamps = block.get("timestamp") or []
    quote = ((block.get("indicators") or {}).get("quote") or [{}])[0]
    adj_block = ((block.get("indicators") or {}).get("adjclose") or [{}])[0]
    adj = adj_block.get("adjclose") or []
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []
    rows = []
    for i, ts in enumerate(stamps):
        if i >= len(opens):
            break
        day = _ymd(ts)
        if day > LAST_DAY:
            continue
        op, hi, lo, cl = _num(opens[i]), _num(highs[i]), _num(lows[i]), _num(closes[i])
        vol = _num(volumes[i]) if i < len(volumes) else None
        if None in (op, hi, lo, cl, vol):
            continue
        if op <= 0 or cl <= 0:
            continue
        adj_c = _num(adj[i]) if i < len(adj) else None
        rows.append({
            "date": day,
            "ticker": ticker,
            "open": op,
            "high": hi,
            "low": lo,
            "close": cl,
            "volume": int(round(vol)),
            "adj_close": adj_c,
        })
    # A duplicate date keeps the later print.
    dedup = {}
    for row in rows:
        dedup[row["date"]] = row
    return [dedup[day] for day in sorted(dedup)]


class ShardWriter:
    def __init__(self):
        self.part = 0
        self.rows: list[dict] = []
        self.manifest = None
        self.manifest_path = None
        self.manifest_rows = 0
        self.files = []
        BARS.mkdir(parents=True, exist_ok=True)
        (BARS / "ohlcv").mkdir(exist_ok=True)
        (BARS / "manifest").mkdir(exist_ok=True)
        self._open_manifest()

    def _open_manifest(self):
        if self.manifest:
            self.manifest.close()
        self.manifest_path = BARS / "manifest" / f"part-{self.part:05d}.jsonl"
        self.manifest = self.manifest_path.open("w", encoding="utf-8")
        self.manifest_rows = 0

    def _flush_parquet(self):
        if not self.rows:
            return
        path = BARS / "ohlcv" / f"part-{self.part:05d}.parquet"
        table = pa.Table.from_pylist(self.rows, schema=SCHEMA)
        pq.write_table(table, path, compression="zstd")
        self.files.append(("ohlcv", path.name, path.stat().st_size, len(self.rows)))
        self.rows = []

    def add(self, row: dict, digest: str, canonical: str):
        if len(self.rows) >= ROWS_PER_PART:
            self._flush_parquet()
            self.manifest.close()
            self.files.append(("manifest", self.manifest_path.name, self.manifest_path.stat().st_size, self.manifest_rows))
            self.part += 1
            self._open_manifest()
        self.rows.append(row)
        # Preregistered line: ticker, date, sha256. The hash is of the
        # 6-decimal canonical bar, which is not repeated here.
        self.manifest.write(
            '{"date":"%s","sha256":"%s","ticker":"%s"}\n' % (row["date"], digest, row["ticker"])
        )
        self.manifest_rows += 1

    def close(self):
        self._flush_parquet()
        if self.manifest:
            self.manifest.close()
            if self.manifest_rows:
                self.files.append((
                    "manifest", self.manifest_path.name,
                    self.manifest_path.stat().st_size, self.manifest_rows,
                ))


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build() -> dict:
    writer = ShardWriter()
    paths = sorted(SRC.glob("*.json"))
    kept = 0
    empty = []
    for i, path in enumerate(paths):
        rows = parse_chart(path)
        if not rows:
            empty.append(path.stem)
            continue
        for row in rows:
            canonical, digest = bar_sha256(
                row["date"], row["ticker"], row["open"], row["high"],
                row["low"], row["close"], row["volume"],
            )
            writer.add(row, digest, canonical)
            kept += 1
        if (i + 1) % 500 == 0:
            print(f"parsed {i + 1}/{len(paths)} files, bars {kept}", flush=True)
    writer.close()
    index = []
    for kind, name, size, nrows in writer.files:
        path = BARS / kind / name
        index.append({
            "kind": kind,
            "name": name,
            "bytes": size,
            "rows": nrows,
            "sha256": file_sha256(path),
        })
    payload = {
        "source": "Yahoo chart v8 quote OHLC, split-adjusted, not dividend-adjusted",
        "signals_use": "open, high, low, close, volume. adj_close is stored and unread.",
        "last_day_inclusive": LAST_DAY,
        "bars": kept,
        "files_read": len(paths),
        "files_empty": empty,
        "shards": index,
        "hash": "sha256 of canonical JSON, sorted keys, no spaces, prices to 6 decimals half away from zero",
    }
    (BARS / "CACHE_INDEX.json").write_text(json.dumps(payload, indent=2) + "\n")
    print(json.dumps({"bars": kept, "empty": len(empty), "shards": len(index)}, indent=2))
    return payload


if __name__ == "__main__":
    build()
