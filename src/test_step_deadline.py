"""step_deadline: per-item shares + narrowed deadlines (sector / catalyst loops)."""
from __future__ import annotations

import os
import sys
import time
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import step_deadline  # noqa: E402


def _with_deadline(seconds_left: float):
    return mock.patch.dict(os.environ, {
        step_deadline.ENV: f"{time.time() + seconds_left:.0f}"})


def test_share_is_none_when_unbounded() -> None:
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop(step_deadline.ENV, None)
        assert step_deadline.share(11, 180, 600) is None


def test_share_splits_evenly_and_clamps() -> None:
    with _with_deadline(2300):
        # 11 sectors: (2300 - 30) / 11 ≈ 206s each — above the floor.
        s = step_deadline.share(11, 180, 600, reserve_s=30)
        assert 200 <= s <= 210
        # 2 left: half of the remainder, clamped to the ceiling.
        assert step_deadline.share(2, 180, 600, reserve_s=30) == 600
        # 1 left: still capped at the ceiling.
        assert step_deadline.share(1, 180, 600, reserve_s=30) == 600
    with _with_deadline(400):
        # Not enough for one even split but above the floor → floor.
        assert step_deadline.share(5, 180, 600, reserve_s=30) == 180
    with _with_deadline(150):
        # Less than the floor left → 0.0, caller must not start.
        assert step_deadline.share(1, 180, 600, reserve_s=30) == 0.0


def test_narrowed_sets_and_restores_and_never_extends() -> None:
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop(step_deadline.ENV, None)
        with step_deadline.narrowed(100):
            rem = step_deadline.remaining_s()
            assert rem is not None and 95 <= rem <= 101
        assert step_deadline.remaining_s() is None
    with _with_deadline(50):
        outer = os.environ[step_deadline.ENV]
        # Asking for more than the parent has cannot push the deadline out.
        with step_deadline.narrowed(500):
            rem = step_deadline.remaining_s()
            assert rem is not None and rem <= 51
        assert os.environ[step_deadline.ENV] == outer
        # None / 0 leave the deadline untouched.
        with step_deadline.narrowed(None):
            assert os.environ[step_deadline.ENV] == outer
        with step_deadline.narrowed(0):
            assert os.environ[step_deadline.ENV] == outer


def test_sector_loop_gives_each_sector_a_slice_and_stops_when_spent() -> None:
    """11 sectors vs a 2400s wall: none may starve the last ones (09-09)."""
    from src import run_sector_predict as rsp

    seen: list[tuple[str, float]] = []
    clock = {"now": 1_000_000.0}

    def fake_time() -> float:
        return clock["now"]

    def fake_run_one(sector, date_str, ch1_md, retries=1, force=False):
        rem = step_deadline.remaining_s()
        seen.append((sector, rem if rem is not None else -1.0))
        # Each sector burns its whole slice (worst case).
        clock["now"] += rem
        return {"quality": "ok"}

    with mock.patch.object(step_deadline.time, "time", fake_time), \
            mock.patch.object(rsp, "run_one", fake_run_one), \
            mock.patch.object(rsp.fetch_channel1, "build",
                              lambda *a, **k: {}), \
            mock.patch.object(rsp.fetch_channel1, "save",
                              lambda *a, **k: None), \
            mock.patch.object(rsp.fetch_channel1, "to_markdown",
                              lambda *a, **k: "ch1"), \
            mock.patch.object(rsp.output_qc, "preopen_report",
                              lambda d: {"sector_n_ok": 11}), \
            mock.patch.object(rsp.output_qc, "write_preopen_report",
                              lambda d: None), \
            mock.patch.object(rsp, "validate", lambda: []), \
            mock.patch.dict(os.environ, {
                step_deadline.ENV: f"{clock['now'] + 2385:.0f}"}), \
            mock.patch.object(sys, "argv", ["x", "--date", "2026-09-10"]):
        wall = os.environ[step_deadline.ENV]
        rsp.main()
        # The step-wide deadline is restored after every narrowed slice.
        assert os.environ[step_deadline.ENV] == wall

    assert len(seen) == len(rsp.FINVIZ_SECTORS) == 11
    # Every sector got at least the floor and no more than the ceiling.
    for sector, rem in seen:
        assert rsp.MIN_SECTOR_S - 1 <= rem <= rsp.MAX_SECTOR_S + 1, (sector, rem)


def test_sector_loop_does_not_start_a_doomed_sector() -> None:
    from src import run_sector_predict as rsp

    started: list[str] = []
    clock = {"now": 2_000_000.0}

    def fake_run_one(sector, *a, **k):
        started.append(sector)
        clock["now"] += 500  # overruns its slice badly
        return {"quality": "ok"}

    with mock.patch.object(step_deadline.time, "time", lambda: clock["now"]), \
            mock.patch.object(rsp, "run_one", fake_run_one), \
            mock.patch.object(rsp.fetch_channel1, "build",
                              lambda *a, **k: {}), \
            mock.patch.object(rsp.fetch_channel1, "save",
                              lambda *a, **k: None), \
            mock.patch.object(rsp.fetch_channel1, "to_markdown",
                              lambda *a, **k: "ch1"), \
            mock.patch.object(rsp.output_qc, "preopen_report",
                              lambda d: {"sector_n_ok": 2}), \
            mock.patch.object(rsp.output_qc, "write_preopen_report",
                              lambda d: None), \
            mock.patch.object(rsp, "validate", lambda: []), \
            mock.patch.dict(os.environ, {
                step_deadline.ENV: f"{clock['now'] + 1200:.0f}"}), \
            mock.patch.object(sys, "argv", ["x", "--date", "2026-09-10"]):
        rsp.main()

    # 1200s wall, 500s per sector → 2 start (1200, 700 left), the third
    # would begin with 200 - 30 reserve < 180 floor → stop, no stub.
    assert started == list(rsp.FINVIZ_SECTORS)[:2]


if __name__ == "__main__":
    for fn in [
        test_share_is_none_when_unbounded,
        test_share_splits_evenly_and_clamps,
        test_narrowed_sets_and_restores_and_never_extends,
        test_sector_loop_gives_each_sector_a_slice_and_stops_when_spent,
        test_sector_loop_does_not_start_a_doomed_sector,
    ]:
        fn()
        print(f"ok {fn.__name__}")
