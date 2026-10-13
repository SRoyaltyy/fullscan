"""Land guard: frozen days are never rewritten; past-day lock is append-only."""
from __future__ import annotations

import gzip
import json

from src.factor_mine_land_guard import (
    LandGuardError, assert_lock_kept, merge_freeze, merge_past_lock,
)


def _doc(code: str, rows=1) -> bytes:
    return json.dumps({"code_sha": code, "date": "2026-10-12", "rows": rows},
                      sort_keys=True, separators=(",", ":")).encode()


def _fail(fragment, fn):
    try:
        fn()
    except LandGuardError as exc:
        assert fragment in str(exc), str(exc)
        return
    raise AssertionError("expected LandGuardError")


def _manifests(up_bytes, me_bytes, slot="lineups", path=None):
    path = path or "data/factor_mine/lineups/2026-10-12.json"
    up = {"version": 1, slot: {"2026-10-12": {"sha256": "aaa", "written_at": "t1"}}}
    me = {"version": 1, slot: {"2026-10-12": {"sha256": "bbb", "written_at": "t2"},
                               "2026-10-13": {"sha256": "ccc"}}}
    return up, me, (lambda p: up_bytes if p == path else None), (lambda p: me_bytes if p == path else None)


def test_frozen_day_kept_byte_for_byte():
    frozen, rebuilt = _doc("b18be514"), _doc("b070934c")
    up, me, ru, rm = _manifests(frozen, rebuilt)
    logs = []
    merged, restore = merge_freeze(up, me, ru, rm, log=logs.append)
    assert merged["lineups"]["2026-10-12"] == {"sha256": "aaa", "written_at": "t1"}
    assert merged["lineups"]["2026-10-13"] == {"sha256": "ccc"}
    assert restore == {"data/factor_mine/lineups/2026-10-12.json": frozen}
    assert any("already frozen — no rewrite 2026-10-12" in l for l in logs)


def test_frozen_ledger_gz_code_sha_only():
    path = "data/factor_mine/ledgers/2026-10-12.json.gz"
    frozen = gzip.compress(_doc("b18be514"), mtime=0)
    rebuilt = gzip.compress(_doc("b070934c"), mtime=0)
    up, me, ru, rm = _manifests(frozen, rebuilt, slot="ledgers", path=path)
    _merged, restore = merge_freeze(up, me, ru, rm, log=lambda _m: None)
    assert restore[path] == frozen


def test_frozen_day_real_mismatch_fails_red():
    up, me, ru, rm = _manifests(_doc("b18be514", rows=1), _doc("b070934c", rows=2))
    _fail("frozen day content mismatch", lambda: merge_freeze(up, me, ru, rm, log=lambda _m: None))


def test_stale_base_missing_day_gets_main_entry():
    frozen = _doc("b18be514")
    up, _me, ru, _rm = _manifests(frozen, None)
    merged, restore = merge_freeze(up, {"version": 1, "lineups": {}}, ru, lambda p: None,
                                   log=lambda _m: None)
    assert merged["lineups"]["2026-10-12"]["sha256"] == "aaa"
    assert restore == {"data/factor_mine/lineups/2026-10-12.json": frozen}


L1 = '{"date":"2026-10-12","kind":"day","name":"yday_gainer_h5","record":"factor_mine","sha256":"80"}'
L2 = '{"date":"2026-10-12","kind":"day","name":"union_hot_n4_h1_preopen","record":"factor_mine","sha256":"30"}'
L3 = '{"date":"2026-10-12","kind":"day","name":"union_hot_n4_holdup_preopen","record":"factor_mine","sha256":"30"}'
L4 = '{"date":"2026-10-13","kind":"day","name":"yday_gainer_h5","record":"factor_mine","sha256":"81"}'


def test_past_lock_keeps_lines_other_jobs_wrote():
    up = "\n".join([L1, L2, L3]) + "\n"
    ours = "\n".join([L1, L4]) + "\n"  # stale base: never saw L2/L3
    out = merge_past_lock(up, ours)
    assert out.splitlines() == [L1, L2, L3, L4]
    assert_lock_kept(up, out)


def test_past_lock_changed_line_fails():
    up = L1 + "\n"
    _fail("would change", lambda: merge_past_lock(up, L1.replace('"80"', '"99"') + "\n"))


def test_past_lock_vanish_detected():
    _fail("vanish", lambda: assert_lock_kept(L1 + "\n" + L2 + "\n", L1 + "\n"))


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
