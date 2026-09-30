"""Calendar-day draw for the Jev trainer. Mixed draw stays in jev_train."""
from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

from .jev_eval import _digest_items, _parsed_items, _row as _eval_row, title_id
from .jev_gate import NEWS_DIR, ROOT, _load_json, _write_json
from .jev_bits import collapse_dupes

DAY_CAP = 500
DAY_RE = re.compile(r"^20\d{2}-\d{2}-\d{2}$")
_SECRET_RE = re.compile(
    r"ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|"
    r"JEV_API_KEY\s*=|sk-[A-Za-z0-9]{24,}"
)


def _reject_secrets(text: str) -> None:
    if _SECRET_RE.search(text or ""):
        raise RuntimeError("refusing to write secret-like material")


def dashboard_days_path(root: Path | None = None) -> Path:
    return (root or ROOT) / "dashboard" / "jev-train" / "days.json"


def _day_token(name: str) -> str:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", name or "")
    return match.group(1) if match else ""


def list_parse_days(news_dir: Path | None = None) -> list[dict]:
    news_dir = news_dir or NEWS_DIR
    by_day: dict[str, dict] = {}

    def bucket(day: str) -> dict:
        return by_day.setdefault(day, {
            "date": day, "parsed": False, "digest": False, "judge": False,
        })

    if news_dir.is_dir():
        for path in sorted(news_dir.iterdir()):
            if not path.is_file():
                continue
            day = _day_token(path.name)
            if not day:
                continue
            name = path.name
            if name.endswith("_parsed.json"):
                bucket(day)["parsed"] = True
            elif "digest" in name and name.endswith(".json"):
                bucket(day)["digest"] = True
            elif name.endswith("_judge.json"):
                bucket(day)["judge"] = True
    return [by_day[k] for k in sorted(by_day)]


def write_days_index(*, root: Path | None = None,
                     news_dir: Path | None = None) -> Path:
    root = root or ROOT
    news_dir = news_dir or (root / "01_daily" / "news")
    path = dashboard_days_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(path, {"schema": "jev-train-days-1", "days": list_parse_days(news_dir)})
    return path


def _judge_titles(blob: dict, day: str) -> list[dict]:
    titles: list[str] = []
    for raw in blob.get("top_items") or []:
        text = str(raw or "").strip()
        if text:
            titles.append(text.split(" | ", 1)[0].strip())
    extra = str(blob.get("rescued_from_noise") or "")
    if extra:
        titles.extend(part.strip() for part in extra.split(";"))
    out: list[dict] = []
    seen: set[str] = set()
    for title in titles:
        row = _eval_row(title, "judge", day, "", day, "judge")
        if not row or row["id"] in seen:
            continue
        seen.add(row["id"])
        out.append(row)
    return out


def load_day_rows(day: str, news_dir: Path | None = None,
                  cap: int = DAY_CAP) -> tuple[list[dict], str]:
    day = (day or "").strip()
    if not DAY_RE.match(day):
        raise RuntimeError(f"day must be YYYY-MM-DD, got {day!r}")
    news_dir = news_dir or NEWS_DIR
    parsed_path = news_dir / f"{day}_parsed.json"
    digest_paths = [
        news_dir / f"{day}_finviz_digest.json",
        news_dir / f"{day}_finviz_market_digest.json",
    ]
    judge_path = news_dir / f"{day}_judge.json"
    rows: list[dict] = []
    kind = ""
    if parsed_path.is_file():
        blob = _load_json(parsed_path)
        rows = _parsed_items(blob, day) if isinstance(blob, dict) else []
        kind = "parsed"
    if not rows:
        for path in digest_paths:
            if not path.is_file():
                continue
            blob = _load_json(path)
            if isinstance(blob, dict):
                rows = _digest_items(blob, day)
                if rows:
                    kind = "digest"
                    break
    if not rows and judge_path.is_file():
        blob = _load_json(judge_path)
        rows = _judge_titles(blob, day) if isinstance(blob, dict) else []
        kind = "judge"
    if not rows:
        raise RuntimeError(f"no parse/digest/judge titles for {day}")
    seen: set[str] = set()
    out: list[dict] = []
    for row in rows:
        tid = row.get("id") or title_id(row.get("title") or "")
        if tid in seen:
            continue
        seen.add(tid)
        item = dict(row)
        item["id"] = tid
        item["pool"] = kind or "day"
        item["date"] = day
        item["url"] = ""
        out.append(item)
        if len(out) >= cap:
            break
    return out, kind or "day"


def run_day_draw(*, day: str, stamp: str = "", write: bool = True,
                 root: Path | None = None) -> dict:
    from .jev_train import (
        allocate_stamp,
        annotate_gate,
        dashboard_draw_path,
        train_dir,
        _print_draw,
        SCHEMA_DRAW,
    )
    now = dt.datetime.now(dt.timezone.utc)
    root = root or ROOT
    news_dir = root / "01_daily" / "news"
    ground = root / "00_grounding"
    day_rows, day_kind = load_day_rows(day, news_dir=news_dir)
    items, model = annotate_gate(
        day_rows, live=False, key="", workers=1, poster=None, asof=now.date(),
    )
    items = collapse_dupes(items)
    directory = train_dir(ground)
    directory.mkdir(parents=True, exist_ok=True)
    used = allocate_stamp(directory, stamp, now, kind="draw")
    report = {
        "schema": SCHEMA_DRAW,
        "stamp": used,
        "generated_at": now.isoformat(),
        "seed": 0,
        "model": model,
        "exam_source": f"day:{day}:{day_kind}",
        "day": day,
        "day_kind": day_kind,
        "day_n": len(day_rows),
        "gate": "hop0-code-bits",
        "sample": {
            "parsed": len(day_rows) if day_kind == "parsed" else 0,
            "rss": 0,
            "exam": 0,
            "exam_source": f"day:{day}:{day_kind}",
            "archive_n": len(day_rows),
            "gold_excluded": 0,
            "trained_excluded": 0,
            "rss_fetched": 0,
            "rss_unique": 0,
            "hard_cursor": 0,
        },
        "items": items,
    }
    _reject_secrets(json.dumps(report))
    if write:
        draw_path = directory / f"{used}_draw.json"
        _write_json(draw_path, report)
        _write_json(dashboard_draw_path(root), report)
        write_days_index(root=root, news_dir=news_dir)
        report["paths"] = {
            "draw": str(draw_path.relative_to(root)),
            "page": "dashboard/jev-train/draw.json",
        }
    _print_draw(report)
    return report


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Jev trainer day draw")
    parser.add_argument("cmd", choices=["draw"])
    parser.add_argument("--day", required=True)
    parser.add_argument("--stamp", default="")
    args = parser.parse_args(argv)
    try:
        run_day_draw(day=args.day, stamp=args.stamp or "", write=True)
    except Exception as exc:  # noqa: BLE001
        print(f"[jev_day] {type(exc).__name__}: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
