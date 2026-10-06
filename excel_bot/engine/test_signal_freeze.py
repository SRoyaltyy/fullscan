"""Verify excel_bot signal days lock from 2026-10-06 and finals stay write-once."""
from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)

import gh_summary  # noqa: E402
import signal_freeze  # noqa: E402

ET = ZoneInfo("America/New_York")
DAY = "2026-10-06"


def _row(**overrides) -> dict:
    base = {
        "run_date": DAY,
        "signal_date": DAY,
        "ticker": "AAA",
        "side": "LONG",
        "strategy": "L1_long_green_tp8_lowvol",
        "exit_rule": "tp8",
        "ref_close": "10.0000",
        "first_open": "",
        "current_price": "10.0000",
        "ret_vs_close": "+0.00%",
        "ret_vs_open": "",
        "days_held": "0",
        "signal_colors": "green|white",
    }
    base.update(overrides)
    return base


def _other(**overrides) -> dict:
    base = _row(
        ticker="BBB",
        strategy="S1_short_red_1day_optionable",
        side="SHORT",
        exit_rule="hold1",
        ref_close="8.0000",
        current_price="8.0000",
        signal_colors="red|white",
    )
    base.update(overrides)
    return base


def _start(tmp: str) -> str:
    path = os.path.join(tmp, "freeze_manifest.json")
    signal_freeze.save_manifest(path, signal_freeze.initial_manifest(), [])
    return path


def _raises(fn) -> str:
    try:
        fn()
    except signal_freeze.SignalFreezeError as exc:
        text = str(exc)
        assert "FAIL CLOSED" in text
        assert "commits nothing" in text
        return text
    raise AssertionError("locked-day violation was accepted")


def _bytes(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


def test_unchanged_passes() -> None:
    """The same picks pass. Live marks do not append a manifest entry."""
    with tempfile.TemporaryDirectory() as tmp:
        path = _start(tmp)
        rows = [_row(), _other()]
        added = signal_freeze.seal(rows, path)
        assert len(added) == 1
        assert added[0]["kind"] == "lock"
        assert added[0]["n_picks"] == 2
        sealed = _bytes(path)
        marked = [
            _row(current_price="11.0000", ret_vs_close="+10.00%", days_held="1"),
            _other(
                current_price="7.5000",
                ret_vs_close="-6.25%",
                ret_vs_open="-7.00%",
                days_held="2",
            ),
        ]
        assert signal_freeze.seal(marked, path) == []
        assert _bytes(path) == sealed
        signal_freeze.verify_store(_write_csv(tmp, marked), path)


def test_changed_pick_fails() -> None:
    """A locked day's picks cannot be changed, added, or removed."""
    with tempfile.TemporaryDirectory() as tmp:
        path = _start(tmp)
        rows = [_row(), _other()]
        signal_freeze.seal(rows, path)
        sealed = _bytes(path)

        def changed():
            bad = [_row(ref_close="99.0000"), _other()]
            signal_freeze.seal(bad, path)

        text = _raises(changed)
        assert "locked fields changed" in text
        assert _bytes(path) == sealed

        def added():
            signal_freeze.seal(rows + [_row(ticker="CCC", ref_close="3.0000")], path)

        text = _raises(added)
        assert "added" in text
        assert _bytes(path) == sealed

        def removed():
            signal_freeze.seal([_row()], path)

        text = _raises(removed)
        assert "removed" in text
        assert _bytes(path) == sealed


def test_first_open_blank_to_value_passes() -> None:
    """A blank first_open may be filled once. The lock entry stays as written."""
    with tempfile.TemporaryDirectory() as tmp:
        path = _start(tmp)
        signal_freeze.seal([_row(), _other()], path)
        before = json.loads(_bytes(path))
        filled = [_row(first_open="10.2500", current_price="10.5000"), _other()]
        added = signal_freeze.seal(filled, path)
        after = json.loads(_bytes(path))
        assert len(added) == 1
        assert added[0]["kind"] == "first_open"
        assert after["entries"][0] == before["entries"][0]
        assert after["entries"][1]["sha256"] == before["entries"][0]["sha256"]
        assert after["entries"][1]["first_opens"] == ["10.2500", ""]
        assert signal_freeze.seal(filled, path) == []
        sealed = _bytes(path)

        def rewritten():
            signal_freeze.seal([
                _row(first_open="10.9999", current_price="10.5000"),
                _other(),
            ], path)

        text = _raises(rewritten)
        assert "10.2500" in text and "10.9999" in text
        assert _bytes(path) == sealed
        signal_freeze.verify_store(_write_csv(tmp, filled), path)


def test_first_open_value_to_other_fails() -> None:
    """A non-blank first_open must not change, including back to blank."""
    with tempfile.TemporaryDirectory() as tmp:
        path = _start(tmp)
        signal_freeze.seal([_row(first_open="10.2500")], path)
        sealed = _bytes(path)

        def other():
            signal_freeze.seal([_row(first_open="11.0000")], path)

        text = _raises(other)
        assert "first_open" in text
        assert "10.2500" in text
        assert _bytes(path) == sealed

        def cleared():
            signal_freeze.seal([_row(first_open="")], path)

        _raises(cleared)
        assert _bytes(path) == sealed


def test_pre_lock_days_are_not_fingerprinted() -> None:
    """Days before 2026-10-06 stay unhashed, even if a pick field changes."""
    with tempfile.TemporaryDirectory() as tmp:
        path = _start(tmp)
        prior = _row(run_date="2026-10-05", signal_date="2026-10-05")
        assert signal_freeze.seal([prior], path) == []
        empty = _bytes(path)
        prior["ticker"] = "ZZZ"
        prior["ref_close"] = "1.0000"
        prior["first_open"] = "1.1000"
        assert signal_freeze.seal([prior], path) == []
        assert _bytes(path) == empty
        manifest = json.loads(empty)
        assert manifest["entries"] == []
        assert manifest["pre_lock"] == "signal_date < 2026-10-06"


def test_manifest_entries_are_not_removed_or_edited() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = _start(tmp)
        signal_freeze.seal([_row()], path)
        signal_freeze.seal([_row(first_open="10.2500")], path)
        original = json.loads(_bytes(path))
        sealed = _bytes(path)
        edited = json.loads(sealed)
        edited["entries"][0]["sha256"] = "0" * 64
        _raises(lambda: signal_freeze.save_manifest(path, edited, original["entries"]))
        assert _bytes(path) == sealed
        dropped = json.loads(sealed)
        dropped["entries"] = dropped["entries"][1:]
        _raises(lambda: signal_freeze.save_manifest(path, dropped, dropped["entries"]))
        assert _bytes(path) == sealed
        assert json.loads(_bytes(path))["entries"][0] == original["entries"][0]


def test_verify_refuses_an_unsealed_day() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = _start(tmp)
        csv_path = _write_csv(tmp, [_row()])
        _raises(lambda: signal_freeze.verify_store(csv_path, path))
        signal_freeze.seal([_row()], path)
        signal_freeze.verify_store(csv_path, path)


def test_committed_manifest_has_no_pre_lock_fingerprints() -> None:
    """The repo manifest does not backfill days before 2026-10-06."""
    manifest = signal_freeze.load_manifest()
    assert manifest["lock_from"] == "2026-10-06"
    assert manifest["pre_lock"] == "signal_date < 2026-10-06"
    assert "pre-lock" in manifest["pre_lock_note"]
    for entry in manifest["entries"]:
        assert entry["signal_date"] >= "2026-10-06"
    signal_freeze.verify_store()


def test_existing_final_file_not_overwritten() -> None:
    """Midday writes the draft. After the close the final is created once."""
    when_mid = datetime(2026, 10, 6, 12, 0, tzinfo=ET)
    when_close = datetime(2026, 10, 6, 16, 5, tzinfo=ET)
    with tempfile.TemporaryDirectory() as tmp:
        sugg = _write_csv(tmp, [_row(), _other(run_date="2026-10-05", signal_date="2026-10-05")])
        out = os.path.join(tmp, "out")
        draft = gh_summary.run(now=when_mid, out_dir=out, sugg_path=sugg)
        final_name = gh_summary.dated_name(DAY, ".md", draft=False)
        draft_name = gh_summary.dated_name(DAY, ".md", draft=True)
        assert os.path.basename(draft) == draft_name
        assert not os.path.exists(os.path.join(out, final_name))
        final = gh_summary.run(now=when_close, out_dir=out, sugg_path=sugg)
        frozen = _bytes(final)
        try:
            gh_summary.run(now=when_close, out_dir=out, sugg_path=sugg)
        except gh_summary.FinalSignalExists as exc:
            assert "REFUSE" in str(exc)
            assert "not overwritten" in str(exc)
        else:
            raise AssertionError("existing final file was overwritten")
        assert _bytes(final) == frozen
        again = gh_summary.run(now=when_mid, out_dir=out, sugg_path=sugg)
        assert os.path.basename(again) == draft_name
        assert _bytes(final) == frozen


def test_run_wires_fail_closed_verify() -> None:
    daily = open(os.path.join(HERE, "daily_run.py"), encoding="utf-8").read()
    plan_at = daily.index("signal_freeze.plan_rows")
    write_at = daily.index("os.replace(tmp, sugg_csv)")
    append_at = daily.index("signal_freeze.append_entries")
    assert plan_at < write_at < append_at
    assert "if not stamp.write_final" in daily
    assert "store_signals(sigs)" in daily
    assert "gh_summary.resolve_session()" in daily
    assert "run_date = stamp.session.isoformat()" in daily
    workflow = open(
        os.path.join(ROOT, ".github", "workflows", "excel_bot.yml"),
        encoding="utf-8",
    ).read()
    run = workflow.split("python engine/daily_run.py", 1)[1]
    assert run.index("signal_freeze.py --verify") < run.index("python engine/gh_summary.py")
    commit = workflow.split("Commit suggestions + daily summary to main", 1)[1]
    assert commit.index("signal_freeze.py --verify") < commit.index("scripts/safe_git_push.sh")
    push_at = commit.index("scripts/safe_git_push.sh")
    assert "freeze_manifest.json" in commit[push_at:]
    assert "write_final" in commit
    assert "FAIL CLOSED" in commit
    assert "exit 1" in commit
    marker = "not committing suggestions.csv or freeze_manifest.json"
    assert marker in commit
    draft_push = commit.split(marker, 1)[1].split("scripts/safe_git_push.sh", 1)[1]
    draft_paths = draft_push.split("||", 1)[0]
    assert "excel_bot/daily/" in draft_paths
    assert "suggestions.csv" not in draft_paths
    assert "freeze_manifest.json" not in draft_paths
    assert 'cron: "30 10 * * 2-6"' in workflow
    assert 'cron: "17 21 * * 1-5"' in workflow
    assert "group: excel-bot" in workflow
    assert "cancel-in-progress: false" in workflow
    assert 'if [ "${GITHUB_EVENT_NAME}" = "schedule" ]; then' in workflow
    assert "LIMIT=0" in workflow
    assert "SIGNALS_ONLY=false" in workflow


def _write_csv(tmp: str, rows: list) -> str:
    path = os.path.join(tmp, "suggestions.csv")
    fieldnames = list(rows[0])
    import csv
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


# 12:53 ET is when run 37499263674 started on 2026-10-06, still before the close.
PRE_CLOSE = datetime(2026, 10, 6, 12, 53, tzinfo=ET)
AFTER_CLOSE = datetime(2026, 10, 6, 17, 17, tzinfo=ET)


def _signal(**overrides) -> dict:
    base = {
        "ticker": "AAA",
        "strategy": "L1_long_green_tp8_lowvol",
        "side": "LONG",
        "exit_rule": "tp8",
        "ref_close": 10.0,
        "signal_date": DAY,
        "signal_colors": "green|white",
    }
    base.update(overrides)
    return base


def _repo_store_files() -> tuple:
    """The real suggestions.csv and freeze_manifest.json this bot commits."""
    return (
        os.path.join(ROOT, "excel_bot", "suggestions", "suggestions.csv"),
        os.path.join(ROOT, "excel_bot", "freeze_manifest.json"),
    )


def _store_stamp(paths) -> tuple:
    return tuple((path, os.stat(path).st_mtime_ns, _bytes(path)) for path in paths)


def _prepare_store(tmp: str) -> tuple:
    """A consistent pre-lock csv plus an empty manifest. Nothing to seal."""
    prior = _row(run_date="2026-10-03", signal_date="2026-10-05")
    return _write_csv(tmp, [prior]), _start(tmp)


def test_pre_close_run_does_not_write_suggestions_csv_or_freeze_manifest() -> None:
    """A New York session before 16:00 ET must not write the store files.

    The clock is gh_summary.resolve_session, same rule as the summary.
    Running the signals/store path leaves suggestions.csv and
    freeze_manifest.json byte-identical — the temp copies and the repo
    files excel_bot/suggestions/suggestions.csv and
    excel_bot/freeze_manifest.json.
    """
    import daily_run
    stamp = gh_summary.resolve_session(PRE_CLOSE)
    assert stamp.session.isoformat() == DAY
    assert stamp.write_final is False
    assert gh_summary.is_after_close(PRE_CLOSE) is False
    real = _repo_store_files()
    real_before = _store_stamp(real)
    with tempfile.TemporaryDirectory() as tmp:
        sugg, manifest = _prepare_store(tmp)
        before = (_bytes(sugg), _bytes(manifest))
        mtimes = (os.stat(sugg).st_mtime_ns, os.stat(manifest).st_mtime_ns)
        daily_run.store_signals(
            [
                _signal(),
                _signal(
                    ticker="BBB",
                    strategy="S1_short_red_1day_optionable",
                    side="SHORT",
                    exit_rule="hold1",
                    ref_close=8.0,
                    signal_colors="red|white",
                ),
            ],
            now=PRE_CLOSE,
            sugg_csv=sugg,
            manifest_path=manifest,
        )
        assert (_bytes(sugg), _bytes(manifest)) == before
        assert (os.stat(sugg).st_mtime_ns, os.stat(manifest).st_mtime_ns) == mtimes
    assert _store_stamp(real) == real_before


def test_final_run_locks_the_closed_session() -> None:
    """After 16:00 ET the store path appends the row and locks the day."""
    import daily_run
    stamp = gh_summary.resolve_session(AFTER_CLOSE)
    assert stamp.write_final is True
    assert stamp.session.isoformat() == DAY
    real_before = _store_stamp(_repo_store_files())
    with tempfile.TemporaryDirectory() as tmp:
        sugg, manifest = _prepare_store(tmp)
        added = daily_run.store_signals(
            [_signal()], now=AFTER_CLOSE, sugg_csv=sugg, manifest_path=manifest,
        )
        assert added == 1
        body = json.loads(_bytes(manifest))
        assert len(body["entries"]) == 1
        entry = body["entries"][0]
        assert entry["kind"] == "lock"
        assert entry["signal_date"] == DAY
        assert entry["n_picks"] == 1
        assert entry["pick_ids"] == [["AAA", "L1_long_green_tp8_lowvol"]]
        assert "AAA" in _bytes(sugg).decode()
        signal_freeze.verify_store(sugg, manifest)
    assert _store_stamp(_repo_store_files()) == real_before


def test_second_final_run_with_identical_picks_verifies() -> None:
    """A second final with the same picks verifies and does not relock."""
    import daily_run
    later = datetime(2026, 10, 6, 18, 0, tzinfo=ET)
    assert gh_summary.resolve_session(later).write_final is True
    with tempfile.TemporaryDirectory() as tmp:
        sugg, manifest = _prepare_store(tmp)
        sigs = [_signal()]
        daily_run.store_signals(
            sigs, now=AFTER_CLOSE, sugg_csv=sugg, manifest_path=manifest,
        )
        locked = _bytes(manifest)
        daily_run.store_signals(
            sigs, now=later, sugg_csv=sugg, manifest_path=manifest,
        )
        assert _bytes(manifest) == locked
        body = json.loads(locked)
        assert len(body["entries"]) == 1
        assert body["entries"][0]["kind"] == "lock"
        signal_freeze.verify_store(sugg, manifest)


def test_changed_picks_on_final_run_fail_closed() -> None:
    """Changed picks after a lock do not rewrite the csv or the manifest."""
    import daily_run
    with tempfile.TemporaryDirectory() as tmp:
        sugg, manifest = _prepare_store(tmp)
        daily_run.store_signals(
            [_signal()], now=AFTER_CLOSE, sugg_csv=sugg, manifest_path=manifest,
        )
        sealed_csv = _bytes(sugg)
        sealed_manifest = _bytes(manifest)

        def changed():
            daily_run.store_signals(
                [_signal(ref_close=99.0)],
                now=datetime(2026, 10, 6, 18, 30, tzinfo=ET),
                sugg_csv=sugg,
                manifest_path=manifest,
            )

        text = _raises(changed)
        assert "cannot be added, removed, or changed" in text
        assert _bytes(sugg) == sealed_csv
        assert _bytes(manifest) == sealed_manifest


VOID_SHA = "9d910cb9de3933b888159d438e95925d105bd8d485b5aaeda60272ad65e44992"
ARCHIVE = os.path.join(ROOT, "excel_bot", "void", "2026-10-06_pre_close_draft.csv")
ARCHIVE_NOTE = os.path.join(ROOT, "excel_bot", "void", "2026-10-06_pre_close_draft.md")


def _manifest_with_void(tmp: str) -> tuple:
    """Real 2026-10-06 lock plus the allowlisted void, in a temp file."""
    manifest = json.loads(json.dumps(signal_freeze.load_manifest()))
    lock = manifest["entries"][0]
    assert lock["kind"] == "lock"
    assert lock["sha256"] == VOID_SHA
    void = signal_freeze.void_entry_for(lock)
    manifest["entries"] = [lock, void]
    path = os.path.join(tmp, "freeze_manifest.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")
    signal_freeze.load_manifest(path)
    return path, lock, void


def test_void_then_relock_passes() -> None:
    """After the allowlisted void, one fresh lock of new picks is accepted."""
    with tempfile.TemporaryDirectory() as tmp:
        path, lock, void = _manifest_with_void(tmp)
        rows = [_row()]
        added = signal_freeze.seal(rows, path, now=AFTER_CLOSE)
        assert len(added) == 1
        assert added[0]["kind"] == "lock"
        assert added[0]["sha256"] != VOID_SHA
        assert added[0]["signal_date"] == DAY
        body = json.loads(_bytes(path))
        assert body["entries"][0] == lock
        assert body["entries"][1]["kind"] == "void"
        assert body["entries"][1]["sha256"] == void["sha256"]
        assert body["entries"][2]["kind"] == "lock"
        assert signal_freeze.seal(rows, path, now=AFTER_CLOSE) == []
        signal_freeze.verify_store(_write_csv(tmp, rows), path)


def test_second_void_fails() -> None:
    """A day can be voided once. A second void does not rewrite the file."""
    with tempfile.TemporaryDirectory() as tmp:
        path, lock, void = _manifest_with_void(tmp)
        sealed = _bytes(path)
        planned = signal_freeze.FreezePlan([void], [lock, void])
        text = _raises(lambda: signal_freeze.append_entries(planned, path, now=AFTER_CLOSE))
        assert "second void" in text
        assert _bytes(path) == sealed


def test_voiding_a_different_sha_fails() -> None:
    """A void that names any sha other than the draft lock is refused."""
    with tempfile.TemporaryDirectory() as tmp:
        path, lock, void = _manifest_with_void(tmp)
        manifest = json.loads(_bytes(path))
        manifest["entries"] = [lock]
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle)
        sealed = _bytes(path)
        bad = dict(void)
        bad["sha256"] = "b" * 64
        text = _raises(lambda: signal_freeze.append_entries(
            signal_freeze.FreezePlan([bad], [lock]), path, now=AFTER_CLOSE,
        ))
        assert "different sha256" in text
        assert _bytes(path) == sealed


def test_change_after_fresh_lock_fails_closed() -> None:
    """Picks changed after the fresh lock do not rewrite the manifest."""
    with tempfile.TemporaryDirectory() as tmp:
        path, lock, _void = _manifest_with_void(tmp)
        rows = [_row()]
        signal_freeze.seal(rows, path, now=AFTER_CLOSE)
        sealed = _bytes(path)

        def changed():
            signal_freeze.seal([_row(ref_close="99.0000")], path, now=AFTER_CLOSE)

        text = _raises(changed)
        assert "cannot be added, removed, or changed" in text
        assert _bytes(path) == sealed
        body = json.loads(sealed)
        assert body["entries"][0] == lock
        assert body["entries"][1]["kind"] == "void"
        assert body["entries"][2]["kind"] == "lock"


def test_void_archive_rows_match_voided_lock_sha() -> None:
    """The archived draft rows are exactly the lock that the void names."""
    assert len(signal_freeze.VOIDABLE_LOCKS) == 1
    allowed = signal_freeze.VOIDABLE_LOCKS[0]
    assert allowed["sha256"] == VOID_SHA
    assert signal_freeze._taken_before_close(allowed["signal_date"], allowed["locked_at"])
    rows = signal_freeze.load_csv(ARCHIVE)
    assert len(rows) == 104
    picks = signal_freeze.canonical_picks(rows)["2026-10-06"]
    assert signal_freeze.fingerprint(picks) == VOID_SHA
    manifest = signal_freeze.load_manifest()
    lock = manifest["entries"][0]
    void = manifest["entries"][1]
    assert lock["kind"] == "lock" and lock["sha256"] == VOID_SHA
    assert void["kind"] == "void"
    assert void["sha256"] == VOID_SHA
    assert void["archive"] == "excel_bot/void/2026-10-06_pre_close_draft.csv"
    assert void["note"] == "excel_bot/void/2026-10-06_pre_close_draft.md"
    assert "pick_ids" not in void
    assert "first_opens" not in void
    assert "n_picks" not in void
    assert void["reason"] == allowed["reason"]
    live = signal_freeze.load_csv()
    assert all(row["signal_date"] != "2026-10-06" for row in live)
    archive_lines = open(ARCHIVE, "rb").read().splitlines(keepends=True)
    live_header = open(
        os.path.join(ROOT, "excel_bot", "suggestions", "suggestions.csv"), "rb",
    ).read().splitlines(keepends=True)[0]
    assert archive_lines[0] == live_header
    assert len(archive_lines) == 105
    note = open(ARCHIVE_NOTE, encoding="utf-8").read()
    assert VOID_SHA in note
    assert "37499263674" in note
    assert "856c5f64c" in note
    assert "13:37" in note
    assert "no book traded on it" in note
    signal_freeze.verify_store()


# sha256 of origin/main's freeze_manifest.json: the single 2026-10-06 lock,
# before this void was appended. json.dumps of that document matches the file.
PRIOR_MANIFEST_SHA256 = "7383a56a06f94b135de6b70c23bd3ad4b76c6d2cc94d73b69a8de2acce68aaac"


def test_save_manifest_appends_without_reserializing_existing_entries() -> None:
    """An append splices the new object. Existing entry text is not rewritten."""
    with tempfile.TemporaryDirectory() as tmp:
        path = _start(tmp)
        signal_freeze.seal([_row()], path, now=AFTER_CLOSE)
        before = _bytes(path).decode("utf-8")
        # A trailing space survives only if the writer does not json.dumps the file.
        mutated = before.replace('"schema": 1,\n', '"schema": 1, \n', 1)
        assert mutated != before
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(mutated)
        signal_freeze.seal([_row(first_open="10.2500")], path, now=AFTER_CLOSE)
        after = _bytes(path).decode("utf-8")
        tail = "\n  ]\n}\n"
        assert mutated.endswith(tail)
        kept = mutated[: -len(tail)]
        assert kept.endswith("\n    }")
        assert after.startswith(kept[:-1] + "},\n")
        assert '"schema": 1, \n' in after
        parsed = json.loads(after)
        assert parsed["entries"][0] == json.loads(mutated)["entries"][0]
        assert parsed["entries"][1]["kind"] == "first_open"


def test_committed_void_leaves_existing_entry_text_unchanged() -> None:
    """The repo manifest's only edit to the old lock text is the joining comma."""
    path = signal_freeze.MANIFEST_PATH
    raw = _bytes(path).decode("utf-8")
    manifest = json.loads(raw)
    assert [entry["kind"] for entry in manifest["entries"]] == ["lock", "void"]
    lock = manifest["entries"][0]
    void = manifest["entries"][1]
    assert "pick_ids" not in void and "first_opens" not in void
    prior = dict(manifest)
    prior["entries"] = [lock]
    prior_text = json.dumps(prior, indent=2, ensure_ascii=False) + "\n"
    assert hashlib.sha256(prior_text.encode("utf-8")).hexdigest() == PRIOR_MANIFEST_SHA256
    assert raw == signal_freeze._splice_entries(prior_text, [void])
    assert prior_text.endswith("\n    }\n  ]\n}\n")
    kept = prior_text[: -len("\n  ]\n}\n")]
    assert kept.endswith("\n    }")
    assert raw.startswith(kept[:-1] + "},\n")
    assert raw.count('"pick_ids"') == prior_text.count('"pick_ids"')


def test_unclosed_signal_date_cannot_be_locked() -> None:
    """signal_freeze itself refuses a lock for a session that has not closed."""
    stamp = gh_summary.resolve_session(PRE_CLOSE)
    assert stamp.write_final is False
    assert stamp.session.isoformat() == DAY
    with tempfile.TemporaryDirectory() as tmp:
        path = _start(tmp)
        sealed = _bytes(path)
        text = _raises(lambda: signal_freeze.seal([_row()], path, now=PRE_CLOSE))
        assert "has not closed" in text
        assert _bytes(path) == sealed
        planned = signal_freeze.FreezePlan(
            [{
                "signal_date": DAY,
                "sha256": "a" * 64,
                "n_picks": 1,
                "pick_ids": [["AAA", "L1_long_green_tp8_lowvol"]],
                "first_opens": [""],
                "kind": "lock",
            }],
            [],
        )
        text = _raises(
            lambda: signal_freeze.append_entries(planned, path, now=PRE_CLOSE)
        )
        assert "has not closed" in text
        assert _bytes(path) == sealed


if __name__ == "__main__":
    test_unchanged_passes()
    test_changed_pick_fails()
    test_first_open_blank_to_value_passes()
    test_first_open_value_to_other_fails()
    test_pre_lock_days_are_not_fingerprinted()
    test_manifest_entries_are_not_removed_or_edited()
    test_verify_refuses_an_unsealed_day()
    test_committed_manifest_has_no_pre_lock_fingerprints()
    test_existing_final_file_not_overwritten()
    test_run_wires_fail_closed_verify()
    test_pre_close_run_does_not_write_suggestions_csv_or_freeze_manifest()
    test_final_run_locks_the_closed_session()
    test_second_final_run_with_identical_picks_verifies()
    test_changed_picks_on_final_run_fail_closed()
    test_unclosed_signal_date_cannot_be_locked()
    test_void_then_relock_passes()
    test_second_void_fails()
    test_voiding_a_different_sha_fails()
    test_change_after_fresh_lock_fails_closed()
    test_void_archive_rows_match_voided_lock_sha()
    test_save_manifest_appends_without_reserializing_existing_entries()
    test_committed_void_leaves_existing_entry_text_unchanged()
    print("ok")
