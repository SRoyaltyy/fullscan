"""Fail unless this change only adds files for start_day_sweep_v1."""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PREFIX = "research/start_day_sweep_v1/"
WORKFLOW = ".github/workflows/start_day_sweep_v1.yml"


def allowed_add(path: str) -> bool:
    if path == WORKFLOW:
        return True
    if not path.startswith(PREFIX):
        return False
    parts = path.split("/")
    return ".." not in parts and path != PREFIX


def parse_name_status(text: str) -> list[tuple[str, str]]:
    rows = []
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        status = parts[0]
        if status.startswith("R") or status.startswith("C"):
            raise SystemExit(f"rename or copy is not append-only: {line}")
        if len(parts) != 2:
            raise SystemExit(f"unexpected diff line: {line}")
        rows.append((status, parts[1]))
    return rows


def judge(rows: list[tuple[str, str]]) -> None:
    if not rows:
        raise SystemExit("diff is empty")
    study = False
    for status, path in rows:
        if status != "A" or not allowed_add(path):
            raise SystemExit(f"not append-only: {status} {path}")
        if path.startswith(PREFIX):
            study = True
    if not study:
        raise SystemExit("study files missing from the diff")


def check_against(rev: str) -> None:
    proc = subprocess.run(
        ["git", "diff", "--name-status", f"{rev}...HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    judge(parse_name_status(proc.stdout))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Append-only check for start_day_sweep_v1.")
    parser.add_argument("--check-against", required=True, metavar="REV")
    args = parser.parse_args(argv)
    check_against(args.check_against)
    print("start_day_sweep_v1 append-only ok")


if __name__ == "__main__":
    main()
