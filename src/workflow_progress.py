"""One progress file per morning or evening run.

``01_daily/_progress/<date>_<workflow>.json`` records each stage as
pending, running, ok, failed, or skipped, plus a heartbeat. The live
page reads that file. A push of the file is rate-limited and can never
fail the job that is writing the packet.

Heartbeat interval is 60 seconds. Remote pushes of heartbeat updates
wait at least ``PUSH_MIN_INTERVAL_S`` so a long sector loop does not
hammer ``safe_git_push.sh``. Stage transitions push immediately.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

HEARTBEAT_S = 60
PUSH_MIN_INTERVAL_S = 60
STALE_S = 180
SCHEMA = 1

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo("America/New_York")
ENV_FILE = "FULLSCAN_PROGRESS_FILE"
ENV_NOPUSH = "FULLSCAN_PROGRESS_NOPUSH"
ENV_PUSH = "FULLSCAN_PROGRESS_PUSH"


def _now_iso(clock=None) -> str:
    if clock is not None:
        value = clock()
        if isinstance(value, str):
            return value
        return value.isoformat(timespec="seconds")
    return datetime.now(ET).isoformat(timespec="seconds")


def progress_path(workflow: str, date: str, root: Path | None = None) -> Path:
    base = root or ROOT
    return base / "01_daily" / "_progress" / f"{date}_{workflow}.json"


def manifest_path(root: Path | None = None) -> Path:
    return (root or ROOT) / "01_daily" / "_progress" / "manifest.json"


def _blank_stage(name: str, model: str) -> dict:
    return {
        "name": name,
        "state": "pending",
        "started_at": "",
        "ended_at": "",
        "heartbeat_at": "",
        "done": 0,
        "total": 1,
        "model": model,
        "error": "",
    }


def _fresh(workflow: str, date: str, stages: list[str], model: str,
           now: str) -> dict:
    return {
        "schema": SCHEMA,
        "workflow": workflow,
        "date": date,
        "run_id": os.environ.get("GITHUB_RUN_ID") or "",
        "model": model,
        "started_at": now,
        "updated_at": now,
        "heartbeat_at": now,
        "stages": [_blank_stage(name, model) for name in stages],
    }


class ProgressWriter:
    """Read-modify-write one run file under an exclusive lock.

    Every public method swallows its own errors. The packet must not
    die because the progress file or the progress push failed.
    """

    def __init__(self, workflow: str, date: str, stages: list[str], *,
                 model: str = "", root: Path | None = None,
                 push_fn=None, clock=None, heartbeat: bool = False,
                 heartbeat_s: float = HEARTBEAT_S):
        self.workflow = workflow
        self.date = date
        self.stage_names = list(stages)
        self.model = model or (
            os.environ.get("LLM_MODEL_OVERRIDE")
            or os.environ.get("LLM_BACKEND")
            or ""
        )
        self.root = root or ROOT
        self.path = progress_path(workflow, date, self.root)
        self.push_fn = push_fn if push_fn is not None else self._git_push
        self.clock = clock
        self.heartbeat_s = heartbeat_s
        self._last_push = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._want_heartbeat = heartbeat

    def begin(self, *, reset: bool = True, heartbeat: bool | None = None) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if reset or not self.path.is_file():
                now = _now_iso(self.clock)
                self._write_raw(_fresh(
                    self.workflow, self.date, self.stage_names, self.model, now))
                self._touch_manifest()
            else:
                self._ensure_stages()
            os.environ[ENV_FILE] = str(self.path)
            if heartbeat if heartbeat is not None else self._want_heartbeat:
                self._start_thread()
        except Exception as exc:  # noqa: BLE001
            print(f"[progress] begin failed: {exc}", flush=True)

    def finish(self) -> None:
        try:
            self._stop.set()
            thread = self._thread
            if thread is not None and thread.is_alive():
                thread.join(timeout=1.0)
            self._thread = None
        except Exception as exc:  # noqa: BLE001
            print(f"[progress] finish failed: {exc}", flush=True)

    def running(self, name: str, *, total: int | None = None,
                model: str | None = None) -> None:
        def edit(stage: dict, data: dict) -> None:
            if stage["state"] != "running":
                stage["started_at"] = stage["started_at"] or _now_iso(self.clock)
            stage["state"] = "running"
            stage["ended_at"] = ""
            if total is not None:
                stage["total"] = int(total)
            if model:
                stage["model"] = model
                if not data.get("model"):
                    data["model"] = model
            self._stamp(stage, data)
        self._edit(name, edit, push="stage")

    def counts(self, name: str, done: int, total: int, *,
               model: str | None = None) -> None:
        def edit(stage: dict, data: dict) -> None:
            stage["done"] = int(done)
            stage["total"] = int(total)
            if model:
                stage["model"] = model
            if stage["state"] == "pending":
                stage["state"] = "running"
                stage["started_at"] = stage["started_at"] or _now_iso(self.clock)
            self._stamp(stage, data)
        self._edit(name, edit, push="heartbeat")

    def ok(self, name: str) -> None:
        self._terminal(name, "ok", "")

    def failed(self, name: str, error: str) -> None:
        self._terminal(name, "failed", error)

    def skipped(self, name: str, error: str = "") -> None:
        self._terminal(name, "skipped", error)

    def note_error(self, name: str, error: str) -> None:
        def edit(stage: dict, data: dict) -> None:
            stage["error"] = (error or "")[:500]
            self._stamp(stage, data)
        self._edit(name, edit, push="heartbeat")

    def heartbeat(self) -> None:
        def edit(stage: dict | None, data: dict) -> None:
            self._stamp(stage, data)
        try:
            self._mutate(lambda data: edit(self._running_stage(data), data))
            self.push(reason="heartbeat")
        except Exception as exc:  # noqa: BLE001
            print(f"[progress] heartbeat failed: {exc}", flush=True)

    def push(self, *, reason: str = "heartbeat") -> bool:
        """Push the progress file. Never raises. Heartbeats are rate-limited."""
        try:
            now = time.monotonic()
            if reason != "stage" and (now - self._last_push) < PUSH_MIN_INTERVAL_S:
                return False
            rels = [self.path]
            manifest = manifest_path(self.root)
            if manifest.is_file():
                rels.append(manifest)
            self.push_fn(rels)
            self._last_push = now
            return True
        except Exception as exc:  # noqa: BLE001
            print(f"[progress] push failed ({reason}): {exc}", flush=True)
            return False

    def _terminal(self, name: str, state: str, error: str) -> None:
        def edit(stage: dict, data: dict) -> None:
            stage["state"] = state
            stage["ended_at"] = _now_iso(self.clock)
            stage["error"] = (error or "")[:500]
            if state == "ok" and stage.get("total"):
                stage["done"] = max(int(stage.get("done") or 0), int(stage["total"]))
            self._stamp(stage, data)
        self._edit(name, edit, push="stage")

    def _stamp(self, stage: dict | None, data: dict) -> None:
        now = _now_iso(self.clock)
        data["updated_at"] = now
        data["heartbeat_at"] = now
        if stage is not None and stage.get("state") == "running":
            stage["heartbeat_at"] = now

    def _running_stage(self, data: dict) -> dict | None:
        for stage in data.get("stages") or []:
            if stage.get("state") == "running":
                return stage
        return None

    def _edit(self, name: str, edit, *, push: str) -> None:
        try:
            def apply(data: dict) -> None:
                stage = self._find(data, name)
                edit(stage, data)
            self._mutate(apply)
            self._touch_manifest()
            self.push(reason=push)
        except Exception as exc:  # noqa: BLE001
            print(f"[progress] update {name} failed: {exc}", flush=True)

    def _find(self, data: dict, name: str) -> dict:
        for stage in data.setdefault("stages", []):
            if stage.get("name") == name:
                return stage
        stage = _blank_stage(name, self.model)
        data["stages"].append(stage)
        return stage

    def _ensure_stages(self) -> None:
        def apply(data: dict) -> None:
            have = {stage.get("name") for stage in data.get("stages") or []}
            for name in self.stage_names:
                if name not in have:
                    data.setdefault("stages", []).append(
                        _blank_stage(name, self.model))
        self._mutate(apply)

    def _mutate(self, apply) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        import fcntl
        with open(self.path, "a+", encoding="utf-8") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                handle.seek(0)
                raw = handle.read()
                if raw.strip():
                    data = json.loads(raw)
                else:
                    data = _fresh(
                        self.workflow, self.date, self.stage_names,
                        self.model, _now_iso(self.clock))
                apply(data)
                text = json.dumps(data, indent=2) + "\n"
                handle.seek(0)
                handle.truncate()
                handle.write(text)
                handle.flush()
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def _write_raw(self, data: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    def _touch_manifest(self) -> None:
        path = manifest_path(self.root)
        path.parent.mkdir(parents=True, exist_ok=True)
        import fcntl
        with open(path, "a+", encoding="utf-8") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                handle.seek(0)
                raw = handle.read()
                try:
                    data = json.loads(raw) if raw.strip() else {}
                except json.JSONDecodeError:
                    data = {}
                if not isinstance(data, dict):
                    data = {}
                runs = [row for row in (data.get("runs") or [])
                        if isinstance(row, dict)]
                filename = self.path.name
                runs = [row for row in runs
                        if not (row.get("date") == self.date
                                and row.get("workflow") == self.workflow)]
                runs.append({
                    "date": self.date,
                    "workflow": self.workflow,
                    "file": filename,
                    "updated_at": _now_iso(self.clock),
                })
                data = {"updated_at": _now_iso(self.clock), "runs": runs[-40:]}
                text = json.dumps(data, indent=2) + "\n"
                handle.seek(0)
                handle.truncate()
                handle.write(text)
                handle.flush()
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def _git_push(self, paths: list[Path]) -> None:
        if os.environ.get(ENV_NOPUSH) == "1":
            return
        in_actions = (os.environ.get("GITHUB_ACTIONS") or "").lower() == "true"
        if not in_actions and os.environ.get(ENV_PUSH) != "1":
            return
        script = self.root / "scripts" / "safe_git_push.sh"
        if not script.is_file():
            return
        rels = []
        for path in paths:
            try:
                rels.append(str(path.relative_to(self.root)))
            except ValueError:
                rels.append(str(path))
        subprocess.run(
            ["bash", str(script),
             f"auto: progress {self.workflow} [{self.date}]", *rels],
            cwd=str(self.root), timeout=120, check=False,
        )

    def _start_thread(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()

        def loop() -> None:
            while not self._stop.wait(self.heartbeat_s):
                self.heartbeat()

        self._thread = threading.Thread(
            target=loop, name=f"progress-{self.workflow}", daemon=True)
        self._thread.start()


class StageBoard:
    """Fold packet step keys into the stages the live page shows."""

    def __init__(self, writer: ProgressWriter, groups: dict[str, list[str]],
                 seal_fn=None):
        self.writer = writer
        self.groups = {name: list(keys) for name, keys in groups.items()}
        self.key_stage = {
            key: stage for stage, keys in self.groups.items() for key in keys
        }
        self.terminal: dict[str, dict[str, tuple[str, str]]] = {
            stage: {} for stage in self.groups
        }
        self.seal_fn = seal_fn

    def on_key(self, key: str, status: str, error: str = "") -> None:
        stage = self.key_stage.get(key)
        if stage is None:
            self.on_stage(key, status, error=error)
            return
        keys = self.groups[stage]
        if status == "running":
            self.writer.running(stage, total=len(keys))
            return
        self.terminal[stage][key] = (status, error or "")
        done = len(self.terminal[stage])
        self.writer.counts(stage, done, len(keys))
        if status == "failed" and error:
            self.writer.note_error(stage, error)
        if done < len(keys):
            return
        statuses = [self.terminal[stage][key_name][0] for key_name in keys]
        errors = [self.terminal[stage][key_name][1] for key_name in keys
                  if self.terminal[stage][key_name][1]]
        if "failed" in statuses:
            self.writer.failed(stage, "; ".join(errors) or "failed")
            return
        if all(item == "skipped" for item in statuses):
            self.writer.skipped(stage, "; ".join(errors))
            return
        self.writer.ok(stage)
        self._seal(stage)

    def on_stage(self, stage: str, status: str, error: str = "",
                 done: int | None = None, total: int | None = None) -> None:
        if status == "running":
            self.writer.running(stage, total=total)
            return
        if done is not None:
            self.writer.counts(stage, done, total or done)
        if status == "ok":
            self.writer.ok(stage)
            self._seal(stage)
        elif status == "failed":
            self.writer.failed(stage, error or "failed")
        elif status == "skipped":
            self.writer.skipped(stage, error)
        else:
            self.writer.note_error(stage, error or status)

    def close_pending(self, keep: set[str] | None = None) -> None:
        keep = keep or set()
        try:
            data = json.loads(self.writer.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        for stage in data.get("stages") or []:
            name = stage.get("name") or ""
            if name in keep:
                continue
            if stage.get("state") == "pending":
                self.writer.skipped(name, "not reached")

    def _seal(self, stage: str) -> None:
        if self.seal_fn is None:
            return
        try:
            self.seal_fn(stage)
        except Exception as exc:  # noqa: BLE001
            print(f"[progress] seal {stage} failed: {exc}", flush=True)


def attach_from_env() -> ProgressWriter | None:
    """Child process (sector loop) updates the file the parent opened."""
    raw = os.environ.get(ENV_FILE) or ""
    if not raw:
        return None
    path = Path(raw)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    names = [stage.get("name") for stage in data.get("stages") or []
             if stage.get("name")]
    writer = ProgressWriter(
        str(data.get("workflow") or "run"),
        str(data.get("date") or ""),
        names,
        model=str(data.get("model") or ""),
        root=_root_from_progress(path),
        heartbeat=False,
    )
    writer.path = path
    return writer


def _root_from_progress(path: Path) -> Path:
    try:
        # 01_daily/_progress/<file>
        return path.parents[2]
    except IndexError:
        return ROOT


def _model_from_env() -> str:
    return (os.environ.get("LLM_MODEL_OVERRIDE")
            or os.environ.get("LLM_BACKEND")
            or "")


def _cli_writer(workflow: str, date: str, stages: list[str]) -> ProgressWriter:
    writer = ProgressWriter(workflow, date, stages, model=_model_from_env())
    writer.begin(reset=not writer.path.is_file(), heartbeat=False)
    return writer


def touch_stage(workflow: str, date: str, stage: str, state: str,
                error: str = "") -> int:
    """Workflow-step bookend. Always returns 0."""
    try:
        writer = _cli_writer(workflow, date, [stage])
        if state == "running":
            writer.running(stage)
        elif state == "ok" or state == "success":
            writer.ok(stage)
        elif state in ("failed", "failure", "cancelled"):
            writer.failed(stage, error or state)
        elif state == "skipped":
            writer.skipped(stage, error)
        else:
            writer.note_error(stage, state)
    except Exception as exc:  # noqa: BLE001
        print(f"[progress] touch failed: {exc}", flush=True)
    return 0


def beat_loop(workflow: str, date: str, stages: list[str], stage: str) -> int:
    """Heartbeat until killed. A push failure does not end the loop."""
    def _stop(signum, _frame) -> None:
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    try:
        writer = ProgressWriter(
            workflow, date, stages or [stage], model=_model_from_env(),
            heartbeat=False)
        writer.begin(reset=not writer.path.is_file(), heartbeat=False)
        writer.running(stage or (stages[0] if stages else "run"))
        while True:
            time.sleep(HEARTBEAT_S)
            writer.heartbeat()
    except (SystemExit, KeyboardInterrupt):
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"[progress] beat ended: {exc}", flush=True)
        return 0


def exec_stage(workflow: str, date: str, stage: str, command: list[str],
               timeout_s: int | None = None) -> int:
    """Run one existing command inside a stage. Progress push cannot change the exit code."""
    writer = ProgressWriter(workflow, date, [stage], model=_model_from_env())
    try:
        writer.begin(reset=not writer.path.is_file(), heartbeat=True)
        writer.running(stage)
    except Exception as exc:  # noqa: BLE001
        print(f"[progress] exec begin failed: {exc}", flush=True)
    code = 1
    try:
        try:
            completed = subprocess.run(command, timeout=timeout_s, check=False)
            code = int(completed.returncode)
        except subprocess.TimeoutExpired:
            code = 124
        except OSError as exc:
            writer.failed(stage, str(exc))
            return 1
        if code == 0:
            writer.ok(stage)
        else:
            writer.failed(stage, f"exit {code}")
    finally:
        writer.finish()
    return code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m src.workflow_progress")
    sub = parser.add_subparsers(dest="cmd")

    touch = sub.add_parser("touch")
    touch.add_argument("--workflow", required=True)
    touch.add_argument("--date", required=True)
    touch.add_argument("--stage", required=True)
    touch.add_argument("--state", required=True)
    touch.add_argument("--error", default="")

    beat = sub.add_parser("beat")
    beat.add_argument("--workflow", required=True)
    beat.add_argument("--date", required=True)
    beat.add_argument("--stages", default="")
    beat.add_argument("--stage", default="")

    exe = sub.add_parser("exec")
    exe.add_argument("--workflow", required=True)
    exe.add_argument("--date", required=True)
    exe.add_argument("--stage", required=True)
    exe.add_argument("--timeout", type=int, default=0)
    exe.add_argument("command", nargs=argparse.REMAINDER)

    args = parser.parse_args(argv)
    if args.cmd == "touch":
        return touch_stage(args.workflow, args.date, args.stage, args.state,
                           args.error)
    if args.cmd == "beat":
        stages = [item for item in args.stages.split(",") if item]
        return beat_loop(args.workflow, args.date, stages, args.stage or (stages[:1] or [""])[0])
    if args.cmd == "exec":
        command = list(args.command)
        if command and command[0] == "--":
            command = command[1:]
        if not command:
            print("[progress] exec needs a command", flush=True)
            return 2
        timeout = args.timeout or None
        return exec_stage(args.workflow, args.date, args.stage, command, timeout)
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
