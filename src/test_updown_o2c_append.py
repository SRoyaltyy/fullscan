"""Sealed updown_o2c_v2 rows stay byte-identical when a later day is appended."""
import gzip
import json
import struct
from pathlib import Path

from src.updown_o2c_append import (
    append_year_blob, read_year, run, sealed_matrix_bytes, sealed_prefix_ok,
)

ROOT = Path(__file__).resolve().parent.parent


def _index(days):
    return {
        "model": "updown_o2c_v2",
        "built": "2026-10-06 18:21 HKT",
        "horizon": "today official open -> today close (o2c)",
        "holdout_start": "2026-07-06",
        "format": "y/YYYY.bin.gz",
        "day_cols": ["date", "period"],
        "tickers": ["AAA", "BBB"],
        "years": ["2026"],
        "days": days,
        "summary": {"dev": {"days": 1}, "holdout": {"days": 1}},
        "notes": ["sealed"],
    }


def _blob(n_t, day_idx, p_rows, r_rows) -> bytes:
    n_d = len(day_idx)
    out = bytearray()
    out += struct.pack("<4sHH", b"UD01", n_t, n_d)
    out += struct.pack("<" + "H" * n_t, *range(n_t))
    out += struct.pack("<" + "H" * n_d, *day_idx)
    for row in p_rows:
        out += struct.pack("<" + "H" * n_t, *row)
    for row in r_rows:
        out += struct.pack("<" + "h" * n_t, *row)
    return bytes(out)


def _write_tree(tmp: Path, days, blob: bytes) -> Path:
    data = tmp / "data"
    (data / "y").mkdir(parents=True)
    (data / "index.json").write_text(
        json.dumps(_index(days), separators=(",", ":")), encoding="utf-8",
    )
    with gzip.GzipFile(filename="", mode="wb", fileobj=(data / "y" / "2026.bin.gz").open("wb"), mtime=0) as handle:
        handle.write(blob)
    return data


def _forward(tmp: Path, day: str, rows) -> Path:
    folder = tmp / "forward"
    folder.mkdir()
    (folder / f"{day}.json").write_text(json.dumps({
        "date": day,
        "rows": rows,
    }), encoding="utf-8")
    return folder


def test_noop_leaves_files_byte_identical(tmp_path: Path):
    blob = _blob(2, [0, 1], [[1000, 2000], [3000, 4000]], [[10, -10], [20, -20]])
    days = [
        ["2026-10-02", "h", 2, 0.5, 0.5, 0.5, 0.1, 0.4],
        ["2026-10-05", "h", 2, 0.5, 0.5, 0.5, 0.1, 0.4],
    ]
    data = _write_tree(tmp_path, days, blob)
    before = {
        path.relative_to(data).as_posix(): path.read_bytes()
        for path in data.rglob("*") if path.is_file()
    }
    report = run(
        data_dir=data, forward_dir=tmp_path / "missing",
        today="2026-11-04", through="2026-11-03", write=True,
    )
    assert report["wrote"] is False
    assert report["sealed_through"] == "2026-10-05"
    assert report["append"] == []
    assert "2026-10-06" in report["missing"]
    assert "2026-11-03" in report["missing"]
    after = {
        path.relative_to(data).as_posix(): path.read_bytes()
        for path in data.rglob("*") if path.is_file()
    }
    assert after == before


def test_append_keeps_sealed_day_and_matrix_bytes(tmp_path: Path):
    p_rows = [[1000, 2000], [3000, 4000]]
    r_rows = [[10, -10], [20, -20]]
    blob = _blob(2, [0, 1], p_rows, r_rows)
    days = [
        ["2026-10-02", "h", 2, 0.5, 0.5, 0.5, 0.1, 0.4],
        ["2026-10-05", "h", 2, 0.5, 0.5, 0.5, 0.1, 0.4],
    ]
    data = _write_tree(tmp_path, days, blob)
    sealed_days = [
        json.dumps(row, separators=(",", ":")) for row in days
    ]
    old_tick, old_day_idx, old_matrix = sealed_matrix_bytes(blob)
    forward = _forward(tmp_path, "2026-10-06", [
        {"ticker": "AAA", "p": 0.62, "r": 0.01},
        {"ticker": "BBB", "p": 0.40, "r": -0.02},
        # A day already sealed must not be a reason to rewrite it.
        # This file is only 2026-10-06.
    ])
    # A second file tries to restamp the sealed day. It must be ignored.
    (forward / "2026-10-05.json").write_text(json.dumps({
        "date": "2026-10-05",
        "rows": [{"ticker": "AAA", "p": 0.99, "r": 0.5}],
    }), encoding="utf-8")
    report = run(
        data_dir=data, forward_dir=forward,
        today="2026-11-04", through="2026-10-06", write=True,
    )
    assert report["wrote"] is True
    assert report["append"] == ["2026-10-06"]
    text = (data / "index.json").read_text(encoding="utf-8")
    for day in sealed_days:
        assert day in text
    assert "2026-10-05" in text
    loaded = json.loads(text)
    assert loaded["days"][0] == days[0]
    assert loaded["days"][1] == days[1]
    assert loaded["days"][2][0] == "2026-10-06"
    assert json.dumps(loaded["days"][0], separators=(",", ":")) == sealed_days[0]
    assert json.dumps(loaded["days"][1], separators=(",", ":")) == sealed_days[1]
    new_blob = read_year(data / "y" / "2026.bin.gz")
    assert sealed_prefix_ok(blob, new_blob)
    new_tick, new_day_idx, new_matrix = sealed_matrix_bytes(new_blob)
    assert new_tick == old_tick
    assert new_day_idx.startswith(old_day_idx)
    old_p_len = len(old_matrix) // 2
    _, n_t, n_d = struct.unpack_from("<4sHH", new_blob, 0)
    new_p = new_matrix[: 2 * n_t * n_d]
    new_r = new_matrix[2 * n_t * n_d:]
    assert new_p[:old_p_len] == old_matrix[:old_p_len]
    assert new_r[:old_p_len] == old_matrix[old_p_len:]
    # The sealed day's stored p was 3000/4000, not the 0.99 from the forward file.
    assert struct.unpack_from("<2H", new_p, 2 * n_t) == (3000, 4000)


def test_day_with_no_source_stays_missing(tmp_path: Path):
    blob = _blob(2, [0], [[1000, 2000]], [[10, -10]])
    days = [["2026-10-05", "h", 2, 0.5, 0.5, 0.5, 0.1, 0.4]]
    data = _write_tree(tmp_path, days, blob)
    forward = _forward(tmp_path, "2026-10-08", [
        ["AAA", 0.55, 0.02],
    ])
    report = run(
        data_dir=data, forward_dir=forward,
        today="2026-11-04", through="2026-10-08", write=True,
    )
    assert report["append"] == ["2026-10-08"]
    assert "2026-10-06" in report["missing"]
    assert "2026-10-07" in report["missing"]
    loaded = json.loads((data / "index.json").read_text(encoding="utf-8"))
    dates = [row[0] for row in loaded["days"]]
    assert dates == ["2026-10-05", "2026-10-08"]


def test_append_year_blob_copies_sealed_bytes():
    blob = _blob(2, [7], [[9, 8]], [[-3, 4]])
    new = append_year_blob(blob, [8], [[1, 2]], [[3, 4]])
    assert sealed_prefix_ok(blob, new)
    tick, day_idx, matrix = sealed_matrix_bytes(blob)
    new_tick, new_day, new_matrix = sealed_matrix_bytes(new)
    assert new_tick == tick
    assert new_day.startswith(day_idx)
    assert new_matrix[:len(matrix) // 2] == matrix[:len(matrix) // 2]


def test_workflow_is_scheduled_on_github_hosted_runners():
    text = (ROOT / ".github" / "workflows" / "updown_o2c_daily.yml").read_text()
    assert 'cron: "45 21 * * 1-5"' in text
    assert "runs-on: ubuntu-latest" in text
    assert "self-hosted" not in text
    commit = text.split("name: Commit", 1)[1]
    assert "expmove" not in commit
    assert "dashboard/updown/data/y" in commit
    assert "research/updown_o2c_v2/forward" in text
    assert "safe_git_push.sh" in text


def test_real_o2c_sealed_prefix_survives_a_forward_day(tmp_path: Path):
    """The published 2026 bin's sealed p/r bytes survive an append."""
    src = ROOT / "dashboard" / "updown" / "data"
    index_text = (src / "index.json").read_text(encoding="utf-8")
    year_gz = (src / "y" / "2026.bin.gz").read_bytes()
    data = tmp_path / "data"
    (data / "y").mkdir(parents=True)
    (data / "index.json").write_text(index_text, encoding="utf-8")
    (data / "y" / "2026.bin.gz").write_bytes(year_gz)
    old_blob = read_year(data / "y" / "2026.bin.gz")
    sealed_days = []
    for row in json.loads(index_text)["days"]:
        if row[0] <= "2026-10-05":
            sealed_days.append(json.dumps(row, separators=(",", ":")))
    forward = _forward(tmp_path, "2026-10-06", [
        {"ticker": "AAPL", "p": 0.51, "r": 0.001},
    ])
    report = run(
        data_dir=data, forward_dir=forward,
        today="2026-11-04", through="2026-10-06", write=True,
    )
    assert report["last_on_disk"] == "2026-10-05"
    assert report["sealed_through"] == "2026-10-05"
    assert report["append"] == ["2026-10-06"]
    text = (data / "index.json").read_text(encoding="utf-8")
    for day in sealed_days:
        assert day in text
    new_blob = read_year(data / "y" / "2026.bin.gz")
    assert sealed_prefix_ok(old_blob, new_blob)
    _, _, old_matrix = sealed_matrix_bytes(old_blob)
    _, n_t, n_d = struct.unpack_from("<4sHH", new_blob, 0)
    _, _, old_n_d = struct.unpack_from("<4sHH", old_blob, 0)
    new_matrix = sealed_matrix_bytes(new_blob)[2]
    old_p_len = 2 * n_t * old_n_d
    new_p_len = 2 * n_t * n_d
    assert new_matrix[:old_p_len] == old_matrix[:old_p_len]
    assert new_matrix[new_p_len:new_p_len + old_p_len] == old_matrix[old_p_len:]
    assert n_d == old_n_d + 1
