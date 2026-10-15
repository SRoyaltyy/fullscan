"""Stage catalog, push gate, and solo rerun for the daily workflows.

A solo rerun (``workflow_dispatch`` input ``stage=<name>``) runs only
that stage's existing modules. It refuses before any write when the
output is sealed, frozen by an input-freeze manifest, or covered by
the past-day lock. It does not backfill a date before the last sealed
day, and it does not add a model call.

The full morning and evening jobs still go through ``run_preopen_all``
and ``run_postclose_all``. This module is the solo path and the shared
description of inputs, outputs, and budgets.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from .workflow_progress import ProgressWriter

ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class StageSpec:
    name: str
    title: str
    budget_s: int
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]


PREOPEN_STAGES: tuple[StageSpec, ...] = (
    StageSpec(
        "prep", "Universe, weather, join, AB", 2090,
        ("data/exports/finviz_{date}.csv",),
        ("data/universe/{date}_membership.csv",
         "01_daily/weather/{date}_weather.json",
         "data/join/{date}_ranked.csv",
         "data/ab_checklist/{date}_ab_checklist_enriched.csv"),
    ),
    StageSpec(
        "news", "Parse, events, actions", 480,
        ("01_daily/news/{date}_finviz_digest.json",
         "01_daily/news/{date}_finviz_market_digest.json",
         "01_daily/map_heat/{date}_map_heat.json"),
        ("01_daily/news/{date}_parsed.json",
         "01_daily/events/{date}_events.json",
         "01_daily/news/{date}_actions.json"),
    ),
    StageSpec(
        "judge", "News judge", 480,
        ("01_daily/news/{date}_parsed.json",
         "01_daily/news/{date}_actions.json"),
        ("01_daily/news/{date}_judge.md",),
    ),
    StageSpec(
        "general_predict", "General market predict", 480,
        ("01_daily/news/{date}_judge.md",
         "01_daily/events/{date}_events.json",
         "01_daily/weather/{date}_weather.json"),
        ("01_daily/general/{date}_predict.md",),
    ),
    StageSpec(
        "sectors", "Per-sector predict (11)", 600,
        ("01_daily/general/{date}_predict.md",),
        ("01_daily/sectors/{date}/",),
    ),
    StageSpec(
        "map_heat", "Map heat morning refresh", 180,
        ("01_daily/map_heat/{date}_research_baseline.json",
         "01_daily/map_heat/{date}_map_heat.json"),
        ("01_daily/map_heat/{date}_research.json",),
    ),
    StageSpec(
        "book_sleeve", "Book, sleeve, paper append", 600,
        ("data/join/{date}_ranked.csv",
         "01_daily/general/{date}_predict.md"),
        ("data/stock_book/{date}_stock_book.json",
         "01_daily/{date}_stock_book.md"),
    ),
    StageSpec(
        "catalyst", "Catalyst dossiers", 3000,
        ("data/stock_book/{date}_stock_book.json",),
        ("01_daily/catalyst/{date}_dossiers.json",),
    ),
    StageSpec(
        "qc", "QC and status", 60,
        ("01_daily/general/{date}_predict.md",),
        ("01_daily/{date}_preopen_qc.json",
         "01_daily/{date}_preopen_status.json"),
    ),
    StageSpec(
        "deploy", "Publish dashboard", 120,
        ("dashboard/",),
        ("dashboard/index.html",),
    ),
)

POSTCLOSE_STAGES: tuple[StageSpec, ...] = (
    StageSpec("outcome", "General outcome", 900,
              ("01_daily/general/{date}_predict.md",),
              ("01_daily/general/{date}_outcome.md",)),
    StageSpec("horizon", "Horizon grade", 60,
              ("01_daily/general/{date}_outcome.md",),
              ("01_daily/general/{date}_horizon.md",)),
    StageSpec("reflect", "General reflect", 900,
              ("01_daily/general/{date}_outcome.md",),
              ("01_daily/general/{date}_reflect.md",)),
    StageSpec("sector_outcomes", "Sector outcomes", 5400,
              ("01_daily/sectors/{date}/",),
              ("01_daily/sectors/{date}/",)),
    StageSpec("sector_reflect", "Sector reflect", 5400,
              ("01_daily/sectors/{date}/",),
              ("01_daily/sectors/{date}/",)),
    StageSpec("learn", "Learn cycle", 600,
              ("01_daily/general/{date}_reflect.md",),
              ("01_daily/{date}_learnings.md",)),
    StageSpec("captains", "Next-session captains", 7200,
              ("01_daily/map_heat/{date}_map_heat.json",),
              ("01_daily/map_heat/",)),
)

PREOPEN_GROUPS = {
    "news": ["news_parse", "events", "events_catcher", "news_actions"],
    "judge": ["news_judge"],
    "general_predict": ["general_predict"],
    "sectors": ["sector_predict"],
    "map_heat": ["map_heat_research"],
    "catalyst": ["catalyst"],
}

POSTCLOSE_GROUPS = {
    "outcome": ["General outcome"],
    "horizon": ["Horizon grade"],
    "reflect": ["General reflect"],
    "sector_outcomes": ["Sector outcomes"],
    "sector_reflect": ["Sector reflect"],
    "learn": ["News actions grader", "HIT board", "Learn cycle",
              "Improvement tracker"],
    "captains": ["Captain research → next session"],
}

# Past-day lock records a solo stage must not rewrite or backfill.
LOCK_RECORDS = {
    ("preopen", "book_sleeve"): ("strategy_tickets", "flatten_robust"),
    ("webull_sim", "rows"): ("webull_sim",),
    ("webull_paper", "preflight"): ("strategy_tickets",),
    ("factor_mine", "land_closed"): ("factor_mine",),
    ("fm_preopen", "book"): ("factor_mine",),
}


def stage_names(workflow: str) -> list[str]:
    if workflow == "preopen":
        return [spec.name for spec in PREOPEN_STAGES]
    if workflow == "postclose":
        return [spec.name for spec in POSTCLOSE_STAGES]
    return []


def push_starts_preopen(event: str, et_hm: int, dow: int, force: bool) -> bool:
    """Whether a Pre-Open trigger may start a packet.

    Schedule and dispatch may. A push may not during 03:00–09:40 ET,
    before 03:55 ET, after 09:25 ET, or on a weekend. ``force`` is the
    only bypass, same as the previous clock gate.
    """
    if force:
        return True
    if event != "push":
        return True
    if int(dow) >= 6:
        return False
    hm = int(et_hm)
    if 300 <= hm < 940:
        return False
    if hm < 355 or hm >= 925:
        return False
    return True


def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def seal_path(workflow: str, date: str, stage: str,
              root: Path | None = None) -> Path:
    base = root or ROOT
    safe = _slug(stage)
    return (base / "01_daily" / "_progress" / "seals"
            / f"{date}_{workflow}_{safe}.json")


def write_seal(workflow: str, date: str, stage: str,
               outputs: list[str], root: Path | None = None) -> Path | None:
    """First successful seal wins. Never rewrite an existing seal."""
    path = seal_path(workflow, date, stage, root)
    if path.is_file():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    from datetime import datetime
    from zoneinfo import ZoneInfo
    body = {
        "workflow": workflow,
        "date": date,
        "stage": stage,
        "sealed_at": datetime.now(ZoneInfo("America/New_York")).isoformat(
            timespec="seconds"),
        "outputs": list(outputs),
    }
    path.write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")
    return path


def _fill(template: str, date: str) -> str:
    return template.format(date=date)


def outputs_for(workflow: str, stage: str, date: str) -> list[str]:
    if stage.startswith("sectors:"):
        sector = stage.split(":", 1)[1].strip()
        slug = _slug(sector)
        return [f"01_daily/sectors/{date}/{slug}_predict.md"]
    specs = PREOPEN_STAGES if workflow == "preopen" else POSTCLOSE_STAGES
    for spec in specs:
        if spec.name == stage:
            return [_fill(item, date) for item in spec.outputs]
    return []


def freeze_reason(outputs: list[str], root: Path) -> str | None:
    """Refuse when an on-disk frozen manifest lists one of these paths.

    Fail-open when the manifest is missing, matching ``freeze_guard``.
    """
    from .freeze_guard import blocked_paths

    def loader(date: str):
        path = root / "research" / "input_freeze" / date / "manifest.json"
        if not path.is_file():
            return None
        try:
            return path.read_bytes()
        except OSError:
            return None

    blocked = blocked_paths(outputs, "HEAD", cwd=root, loader=loader)
    if not blocked:
        return None
    listed = ", ".join(sorted(blocked))
    return f"refused: frozen input-freeze path {listed}"


def past_lock_reason(workflow: str, stage: str, date: str,
                     root: Path) -> str | None:
    records = LOCK_RECORDS.get((workflow, stage))
    if not records:
        return None
    manifest = root / "data" / "past_day_lock" / "manifest.jsonl"
    if not manifest.is_file():
        return None
    rows = []
    try:
        for line in manifest.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    except OSError:
        return None
    for record in records:
        days = [str(row.get("date") or "") for row in rows
                if row.get("kind") == "day" and row.get("record") == record
                and row.get("date")]
        if date in days:
            return (f"refused: past-day lock seals {record} {date}. "
                    "Not rescoring a sealed day.")
        if days and date < max(days):
            return (f"refused: {date} is before the last sealed {record} "
                    f"day {max(days)}. A missed day stays missing.")
    return None


def quality_reason(workflow: str, stage: str, date: str,
                   root: Path) -> str | None:
    """A quality-ok primary output is sealed for a solo rerun."""
    if workflow != "preopen":
        return None
    try:
        from . import output_qc
    except Exception:  # noqa: BLE001
        return None
    if stage == "general_predict":
        path = root / "01_daily" / "general" / f"{date}_predict.md"
        if path.is_file() and output_qc.qc_general_predict(path).ok:
            return ("refused: general predict is quality-ok and will not "
                    "be overwritten")
    if stage == "judge":
        path = root / "01_daily" / "news" / f"{date}_judge.md"
        if path.is_file() and output_qc.qc_news_judge(path).ok:
            return "refused: news judge is quality-ok and will not be overwritten"
    if stage.startswith("sectors:"):
        rel = outputs_for(workflow, stage, date)[0]
        path = root / rel
        if path.is_file() and output_qc.qc_sector_predict(path).ok:
            return "refused: sector essay is quality-ok and will not be overwritten"
    return None


def sealed_reason(workflow: str, stage: str, date: str, *,
                  root: Path | None = None) -> str | None:
    """Why a solo rerun must not start, or None when it may write."""
    base = root or ROOT
    path = seal_path(workflow, date, stage, base)
    if path.is_file():
        return f"refused: {workflow} {stage} {date} is sealed ({path.name})"
    if stage.startswith("sectors:"):
        parent = seal_path(workflow, date, "sectors", base)
        if parent.is_file():
            return (f"refused: sectors {date} is sealed; "
                    f"not overwriting {stage}")
    outputs = outputs_for(workflow, stage, date)
    reason = freeze_reason(outputs, base) if outputs else None
    if reason:
        return reason
    reason = past_lock_reason(workflow, stage, date, base)
    if reason:
        return reason
    return quality_reason(workflow, stage, date, base)


def maybe_seal(workflow: str, stage: str, date: str,
               root: Path | None = None) -> None:
    """Seal a stage once its primary output is quality-ok. Never raises."""
    try:
        if sealed_reason(workflow, stage, date, root=root):
            write_seal(workflow, date, stage,
                       outputs_for(workflow, stage, date), root)
            return
        if quality_reason(workflow, stage, date, root or ROOT):
            write_seal(workflow, date, stage,
                       outputs_for(workflow, stage, date), root)
    except Exception as exc:  # noqa: BLE001
        print(f"[stages] seal {stage} failed: {exc}", flush=True)


class SoloRefused(SystemExit):
    """Solo rerun stopped before it could overwrite a locked output."""


def run_solo(workflow: str, stage: str, date: str, *,
             runner, root: Path | None = None, model: str = "") -> int:
    """Run ``runner`` only when the stage is not sealed.

    ``runner`` is called with no arguments and returns a process exit
    code. When the stage is sealed, ``runner`` is not called.
    """
    base = root or ROOT
    names = stage_names(workflow) or [stage.split(":", 1)[0]]
    if stage not in names and not stage.startswith("sectors:"):
        names = list(names) + [stage]
    writer = ProgressWriter(
        workflow, date, names, model=model, root=base, heartbeat=False)
    writer.begin(reset=False, heartbeat=False)
    reason = sealed_reason(workflow, stage, date, root=base)
    if reason:
        print(f"[stages] {reason}", flush=True)
        writer.failed(stage, reason)
        raise SoloRefused(2)
    writer.running(stage)
    code = int(runner())
    if code == 0:
        writer.ok(stage)
        maybe_seal(workflow, stage, date, base)
    elif code == 3:
        writer.skipped(stage, "step deadline")
        return 0
    else:
        writer.failed(stage, f"exit {code}")
    return code


def _py() -> str:
    return sys.executable


def _force_flag(force: bool) -> list[str]:
    return ["--force"] if force else []


def preopen_commands(stage: str, date: str, force: bool) -> list[tuple[list[str], int | None]]:
    """The same modules the full packet runs, for one stage only."""
    from .run_preopen_all import _essay_timeout, _step_budget

    py = _py()
    fa = _force_flag(force)
    morning = os.environ.get("PREOPEN_LLM_TIMEOUT", "420")
    try:
        ceiling = max(120, int(morning) + 60)
    except ValueError:
        ceiling = 480

    def essay(n_left: int, module: str, extra: list[str] | None = None):
        budget = _essay_timeout(n_left, ceiling)
        argv = [py, "-m", module, "--date", date, *fa, *(extra or [])]
        return argv, budget

    if stage == "prep":
        return [
            ([py, "-m", "src.segments", "--date", date], 180),
            ([py, "-m", "src.weather", "--date", date], 50),
            ([py, "-m", "src.join", "--date", date], 180),
            ([py, "-m", "src.ab_checklist", "--date", date], 1500),
            ([py, "-m", "src.ab_enrich", "--date", date], 180),
        ]
    if stage == "news":
        commands = [
            ([py, "-m", "src.news_parse", "--hours", "48", "--limit", "400",
              "--date", date, *fa], 120),
        ]
        for n_left, module in (
                (3, "src.run_events"),
                (2, "src.run_events_catcher"),
                (1, "src.news_actions")):
            if module == "src.news_actions":
                argv, budget = essay(n_left, module, ["--hours", "48", "--limit", "400"])
            else:
                argv, budget = essay(n_left, module)
            commands.append((argv, budget))
        return commands
    if stage == "judge":
        return [essay(1, "src.run_news_judge")]
    if stage == "general_predict":
        return [essay(1, "src.run_predict")]
    if stage == "sectors" or stage.startswith("sectors:"):
        argv = [py, "-m", "src.run_sector_predict", "--date", date, *fa]
        if stage.startswith("sectors:"):
            argv.extend(["--sectors", stage.split(":", 1)[1].strip()])
        return [(argv, _step_budget(600, floor=60))]
    if stage == "map_heat":
        budget = _step_budget(180, floor=60)
        if budget is None:
            return [([py, "-m", "src.map_heat_refresh", "--date", date,
                      "--passthrough", *fa], 60)]
        return [([py, "-m", "src.map_heat_refresh", "--date", date, *fa], budget)]
    if stage == "book_sleeve":
        return [
            ([py, "-m", "src.run_stock_book_all", "--date", date,
              "--skip-llm", "--skip-extras", "--top", "25", *fa], None),
            ([py, "-m", "src.morning_look", "live_boards", "--date", date,
              "--write", "--no-extras"], _step_budget(120)),
            ([py, "-m", "src.sleeve_combine_bt", "--mode", "io_boost",
              "--hold", "3d"], _step_budget(180)),
            ([py, "-m", "src.sleeve_merge", "--card", "--date", date,
              "--write-card"], _step_budget(120)),
            ([py, "-m", "src.paper_trade_append", "--date", date, "--top", "10"],
             _step_budget(180)),
        ]
    if stage == "catalyst":
        from . import catalyst_daily
        return [([py, "-m", "src.catalyst_daily", "--date", date, *fa],
                 catalyst_daily.CATALYST_STEP_S)]
    if stage == "qc":
        return [([py, "-m", "src.output_qc", "--date", date, "--preopen"], 60)]
    if stage == "deploy":
        # The workflow publish step is the deploy. This command does not
        # rewrite the packet; it only lets a solo dispatch reach that step.
        return [([py, "-c",
                  "print('[stages] deploy hands off to publish_dashboard.sh')"],
                 30)]
    raise SystemExit(f"unknown preopen stage {stage}")


def postclose_commands(stage: str, date: str, force: bool
                       ) -> list[tuple[list[str], int | None]]:
    py = _py()
    fa = _force_flag(force)
    table = {
        "outcome": [([py, "-m", "src.run_outcome", "--date", date, *fa], 900)],
        "horizon": [([py, "-m", "src.horizon_grade", "--date", date], 60)],
        "reflect": [([py, "-m", "src.run_reflect", "--date", date, *fa], 900)],
        "sector_outcomes": [([py, "-m", "src.run_sector_outcome", "--date", date, *fa], 5400)],
        "sector_reflect": [([py, "-m", "src.run_sector_reflect", "--date", date, *fa], 5400)],
        "learn": [
            ([py, "-m", "src.news_grade"], 300),
            ([py, "-m", "src.hit_board"], 60),
            ([py, "-m", "src.learn_cycle", "--date", date], 180),
            ([py, "-m", "src.improvement_tracker"], 60),
        ],
        "captains": [([py, "-m", "src.map_heat_postclose", "--source-date", date], 7200)],
    }
    if stage not in table:
        raise SystemExit(f"unknown postclose stage {stage}")
    return table[stage]


def _default_runner(workflow: str, stage: str, date: str, force: bool):
    if workflow == "preopen":
        commands = preopen_commands(stage, date, force)
    elif workflow == "postclose":
        commands = postclose_commands(stage, date, force)
    else:
        raise SystemExit(f"solo rerun is not wired for {workflow}")

    def run() -> int:
        ran = False
        for argv, timeout_s in commands:
            # None is the deadline miss from _essay_timeout / _step_budget.
            if timeout_s is None:
                print(f"[stages] skip (deadline) {' '.join(argv)}", flush=True)
                continue
            ran = True
            print(f"[stages] {' '.join(argv)} timeout={timeout_s}", flush=True)
            try:
                completed = subprocess.run(argv, timeout=timeout_s, check=False)
            except subprocess.TimeoutExpired:
                return 124
            if completed.returncode != 0:
                return int(completed.returncode)
        if not ran:
            return 3
        if workflow == "preopen" and stage == "general_predict":
            from .run_preopen_all import _heal_general_predict, _step_budget
            budget = _step_budget(420, floor=90)
            if budget is not None:
                _heal_general_predict(
                    date,
                    [_py(), "-m", "src.run_predict", "--date", date,
                     *_force_flag(force)],
                    budget)
        return 0

    return run


def execute_solo(workflow: str, stage: str, date: str, force: bool = False,
                 root: Path | None = None) -> int:
    # Rebuild commands so a None timeout (deadline miss) is a skip, while
    # book steps that have no wall clock use a large existing cap.
    if workflow == "preopen" and stage == "book_sleeve":
        built = preopen_commands(stage, date, force)
        # run_stock_book_all has no numeric cap in the full job (the job
        # timeout covers it). _step_budget None means skip the tail only.
        fixed = []
        for argv, timeout_s in built:
            if timeout_s is None and "run_stock_book_all" in " ".join(argv):
                fixed.append((argv, 2400))
            else:
                fixed.append((argv, timeout_s))

        def runner() -> int:
            ran = False
            for argv, timeout_s in fixed:
                if timeout_s is None:
                    print(f"[stages] skip (deadline) {' '.join(argv)}", flush=True)
                    continue
                ran = True
                completed = subprocess.run(argv, timeout=timeout_s, check=False)
                if completed.returncode != 0:
                    return int(completed.returncode)
            return 0 if ran else 3
    else:
        runner = _default_runner(workflow, stage, date, force)
    try:
        return run_solo(workflow, stage, date, runner=runner, root=root)
    except SoloRefused:
        return 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m src.workflow_stages")
    sub = parser.add_subparsers(dest="cmd")

    gate = sub.add_parser("gate")
    gate.add_argument("--event", required=True)
    gate.add_argument("--et-hm", required=True, type=int)
    gate.add_argument("--dow", required=True, type=int)
    gate.add_argument("--force", default="false")

    run = sub.add_parser("run")
    run.add_argument("--workflow", required=True)
    run.add_argument("--stage", required=True)
    run.add_argument("--date", required=True)
    run.add_argument("--force", action="store_true")

    args = parser.parse_args(argv)
    if args.cmd == "gate":
        force = str(args.force).lower() in ("1", "true", "yes")
        if push_starts_preopen(args.event, args.et_hm, args.dow, force):
            return 0
        print(f"[stages] push gate closed event={args.event} "
              f"et_hm={args.et_hm} dow={args.dow}")
        return 1
    if args.cmd == "run":
        return execute_solo(args.workflow, args.stage, args.date, args.force)
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
