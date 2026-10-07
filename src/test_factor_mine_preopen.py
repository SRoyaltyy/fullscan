"""Pre-open sleeves: commit-time gate, seal, sequential book.

Run: python -m src.test_factor_mine_preopen
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

from src import factor_mine as fm
from src import factor_mine_preopen as fp

D1, D2 = "2026-10-08", "2026-10-09"
NAMES = list(fp.SLEEVES)


def _commit(repo: Path, rel: str, body: str, when: str) -> None:
    p = repo / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(body, encoding="utf-8")
    env = {**os.environ, "GIT_COMMITTER_DATE": when, "GIT_AUTHOR_DATE": when}
    subprocess.run(["git", "add", rel], cwd=repo, check=True)
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t",
                    "commit", "-qm", rel], cwd=repo, check=True, env=env)


def _repo() -> Path:
    repo = Path(tempfile.mkdtemp())
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    return repo


def _tickets(day: str, source: str, buys: list[str]) -> str:
    rows = [{"ticker": t, "src": "ohlc_hot"} for t in buys]
    strat = {p: {"buy": rows} for p in fp.SLEEVES.values()}
    return json.dumps({"date": day, "look": {"source": source}, "strategies": strat})


def _predict(s: float) -> str:
    return f"Prediction: UP (total score {s})\n"


def test_gate_uses_last_pre_open_commit() -> None:
    repo = _repo()
    rel = fp.tickets_rel(D1)
    _commit(repo, rel, _tickets(D1, "look", ["AAA"]), "2026-10-08T09:00:00-04:00")
    _commit(repo, rel, _tickets(D1, "look", ["ZZZ"]), "2026-10-08T09:41:00-04:00")
    info = fp.GitClock(repo).before(rel, fp.open_at(D1))
    assert info["ok"] and b"AAA" in info["raw"], info
    late = fp.tickets_rel(D2)
    _commit(repo, late, _tickets(D2, "look", ["BBB"]), "2026-10-09T09:31:00-04:00")
    info = fp.GitClock(repo).before(late, fp.open_at(D2))
    assert not info["ok"] and "not before 09:30" in info["reason"], info


def test_seal_fails_closed() -> None:
    repo = _repo()
    _commit(repo, fp.tickets_rel(D1), _tickets(D1, "no_same_day_panel", []),
            "2026-10-08T05:40:00-04:00")
    _commit(repo, fp.predict_rel(D1), _predict(1.0), "2026-10-08T05:00:00-04:00")
    doc = fp.build_seal(D1, clock=fp.GitClock(repo))
    for n in NAMES:
        e = doc["sleeves"][n]
        assert e["sit"] and e["picks"] == []
        assert any("look.source=no_same_day_panel" in r for r in e["reasons"])
    # red morning
    repo = _repo()
    _commit(repo, fp.tickets_rel(D1), _tickets(D1, "look", ["AAA"]),
            "2026-10-08T05:40:00-04:00")
    _commit(repo, fp.predict_rel(D1), _predict(-3.5), "2026-10-08T05:00:00-04:00")
    doc = fp.build_seal(D1, clock=fp.GitClock(repo))
    assert all(doc["sleeves"][n]["sit"] for n in NAMES)
    # S committed after the open is not used
    repo = _repo()
    _commit(repo, fp.tickets_rel(D1), _tickets(D1, "look", ["AAA"]),
            "2026-10-08T05:40:00-04:00")
    _commit(repo, fp.predict_rel(D1), _predict(2.0), "2026-10-08T09:45:00-04:00")
    doc = fp.build_seal(D1, clock=fp.GitClock(repo))
    assert doc["s"] is None and all(doc["sleeves"][n]["sit"] for n in NAMES)


def test_seal_refuses_after_open_and_before_start() -> None:
    d = Path(tempfile.mkdtemp())
    late = datetime(2026, 10, 8, 9, 30, tzinfo=fp.ET)
    assert fp.seal(D1, now=late, seal_dir=d) is None
    assert fp.seal("2026-10-07", now=datetime(2026, 10, 7, 8, 0, tzinfo=fp.ET),
                   seal_dir=d) is None
    assert not list(d.iterdir())


def test_book_sequential_append_only() -> None:
    repo = _repo()
    _commit(repo, fp.tickets_rel(D1), _tickets(D1, "look", ["AAA", "BBB"]),
            "2026-10-08T05:40:00-04:00")
    _commit(repo, fp.predict_rel(D1), _predict(1.0), "2026-10-08T05:00:00-04:00")
    clock = fp.GitClock(repo)
    seal = fp.build_seal(D1, clock=clock)
    assert seal["sleeves"][NAMES[0]]["picks"] == ["AAA", "BBB"]
    _commit(repo, fp.seal_rel(D1), fp._dumps(seal), "2026-10-08T08:10:00-04:00")
    # D2: seal committed after the open -> sit
    _commit(repo, fp.seal_rel(D2), fp._dumps({**seal, "date": D2}),
            "2026-10-09T09:40:00-04:00")
    bars = {
        ("AAA", D1): {"open": 10.0, "high": 11, "low": 9, "close": 10.5},
        ("BBB", D1): {"open": 20.0, "high": 21, "low": 19, "close": 19.5},
        ("AAA", D2): {"open": 10.6, "high": 11, "low": 9, "close": 10.7},
        ("BBB", D2): {"open": 19.0, "high": 21, "low": 19, "close": 19.5},
    }
    real = fm.session_has_closed
    fm.session_has_closed = lambda d, now=None: True
    try:
        st, sc = Path(tempfile.mkdtemp()), Path(tempfile.mkdtemp())
        out = fp.book(D2, bars=bars, clock=clock, state_dir=st, score_dir=sc,
                      lock=False, summary_path=st / 's.json')
        for n in NAMES:
            recs = out[n]
            assert [r["date"] for r in recs] == [D1, D2]
            assert sorted(b["ticker"] for b in recs[0]["buys"]) == ["AAA", "BBB"]
            assert recs[1]["buys"] == [] and recs[1]["sit"]
            assert any("09:30" in r for r in recs[1]["reasons"])
            md = (sc / f"{n}.md").read_text()
            assert fp.NEW_LABEL in md and f"| {D1} |" in md
        h1 = out["union_hot_n4_h1_preopen"][1]
        assert sorted(s["ticker"] for s in h1["sells"]) == ["AAA", "BBB"]
        hold = out["union_hot_n4_holdup_preopen"][1]
        assert hold["sells"] == [] and hold["holdings"] == ["AAA", "BBB"]
        again = fp.book(D2, bars=bars, clock=clock, state_dir=st, score_dir=sc,
                        lock=False, summary_path=st / 's.json')
        assert again[NAMES[0]][-1] == out[NAMES[0]][-1]
    finally:
        fm.session_has_closed = real


def main() -> None:
    test_gate_uses_last_pre_open_commit()
    test_seal_fails_closed()
    test_seal_refuses_after_open_and_before_start()
    test_book_sequential_append_only()
    print("ok factor_mine_preopen")


if __name__ == "__main__":
    main()
