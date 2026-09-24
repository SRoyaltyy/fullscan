"""Draw up to 100 articles and run the Lane one-shot stack.

Usage:
  python3 -m src.lane_one_shot --env-check
  python3 -m src.lane_one_shot --print-budgets --run-budget-minutes 540 --deadline-et 03:30
  python3 -m src.lane_one_shot --shard 0 --shards 5 --target 20 --max-draws 80 --budget-seconds 4860
  python3 -m src.lane_one_shot --merge artifacts/ --shard-count 5

Refuses to publish a row whose watermark is not lane::<provider>::<model>.
Does not call classify.py or families.analyze.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import re
import signal
import time
import zlib
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from src import lane_route as lane
from src.news_impact.axioms import load_axioms
from src.news_impact.finviz_linker import get_index
from src.news_impact.action_grade import grade_action_rows, summarize_tape
from src.news_impact.one_shot_stack import (
    GOLD_EXTRA,
    GOLD_KEEP,
    GOLD_REJECT,
    analyst_repair_note,
    classify_repair_note,
    filter_repair_note,
    gate0,
    process_article,
    render_markdown,
)
from src.news_impact.scratch import article_id

SCRATCH = Path("02_lessons/lane/one_shot_100")
BOARD = Path("03_scoreboard/LANE_ONE_SHOT_100.md")
NEWS = Path("01_daily/news")
EXPORTS = Path("data/exports")
ELITE = Path("data/theme_radar_snapshots")

ENV_ROWS = [
    "ZHIPU_API_KEY", "GLM_API_KEY", "SILICONFLOW_API_KEY", "OPENROUTER_API_KEY",
    "DASHSCOPE_API_KEY", "DASHSCOPE_BASE_URL", "QWEN_API_KEY",
    "TOKENHUB_API_KEY", "TENCENT_API_KEY", "HUNYUAN_API_KEY",
    "TOKENHUB_BASE_URL", "TENCENT_BASE_URL",
    "GEMINI_API_KEY", "GOOGLE_AI_STUDIO_API_KEY",
    "MOONSHOT_API_KEY", "MODELSCOPE_API_KEY", "MODELSCOPE_API_TOKEN",
    "MODELSCOPE_SDK_TOKEN", "MISTRAL_API_KEY", "NVIDIA_NIM_API_KEY",
    "NVIDIA_API_KEY", "POLLINATIONS_API_KEY", "GROQ_API_KEY", "HF_TOKEN",
    "GITHUB_MODELS_TOKEN", "SAMBANOVA_API_KEY", "CLOUDFLARE_API_TOKEN",
    "CLOUDFLARE_ACCOUNT_ID", "OLLAMA_URL", "DEEPSEEK_API_KEY",
    "LANE_ALLOW_PAID_DEEPSEEK", "LANE_URL",
    "OPENCLAW_GATEWAY_URL", "OPENCLAW_TOKEN", "OPENCLAW_BACKEND_MODEL",
]
# Automatic Actions GITHUB_TOKEN is not a Lane secret.
KEY_VARS = [
    "ZHIPU_API_KEY", "GLM_API_KEY", "SILICONFLOW_API_KEY", "OPENROUTER_API_KEY",
    "DASHSCOPE_API_KEY", "QWEN_API_KEY", "TOKENHUB_API_KEY", "TENCENT_API_KEY",
    "HUNYUAN_API_KEY", "GEMINI_API_KEY", "GOOGLE_AI_STUDIO_API_KEY",
    "MOONSHOT_API_KEY", "MODELSCOPE_API_KEY", "MODELSCOPE_API_TOKEN",
    "MODELSCOPE_SDK_TOKEN", "MISTRAL_API_KEY", "NVIDIA_NIM_API_KEY",
    "NVIDIA_API_KEY", "POLLINATIONS_API_KEY", "GROQ_API_KEY", "HF_TOKEN",
    "GITHUB_MODELS_TOKEN", "SAMBANOVA_API_KEY",
]

_WM = re.compile(r"^lane::[a-z0-9_]+::\S+$")


def env_lines() -> list[str]:
    lines = []
    for name in ENV_ROWS:
        raw = os.environ.get(name) or ""
        val = raw.strip()
        if val:
            lines.append(f"present {name} len={len(val)}")
        else:
            lines.append(f"missing {name}")
    return lines


def secrets_ready() -> bool:
    if any((os.environ.get(name) or "").strip() for name in KEY_VARS):
        return True
    if (os.environ.get("CLOUDFLARE_API_TOKEN") or "").strip() and (
        os.environ.get("CLOUDFLARE_ACCOUNT_ID") or ""
    ).strip():
        return True
    if (os.environ.get("OLLAMA_URL") or "").strip():
        return True
    if (os.environ.get("OPENCLAW_GATEWAY_URL") or "").strip():
        return True
    return False


def print_env() -> bool:
    print("[lane_one_shot] env (redacted)")
    for line in env_lines():
        print(" ", line)
    ready = secrets_ready()
    print("[lane_one_shot] hopper_secrets", "present" if ready else "missing")
    return ready


def _norm(title: str) -> str:
    text = re.sub(r"[^a-z0-9\s]", " ", (title or "").lower())
    return re.sub(r"\s+", " ", text).strip()[:180]


def _add(bag: dict[str, dict], title: str, body: str, source: str,
         known: str, hint: str, harvest: str) -> None:
    title = (title or "").strip()
    if len(title) < 20:
        return
    key = _norm(title)
    if not key or key in bag:
        return
    bag[key] = {
        "title": title[:300],
        "body": (body or "")[:800],
        "known_at": known,
        "source_file": source,
        "ticker_hint": (hint or "").upper().strip(),
        "harvest_source": harvest,
        "article_id": article_id(title, known),
    }


def harvest(root: Path | None = None) -> list[dict]:
    root = root or Path(".")
    bag: dict[str, dict] = {}
    news = root / NEWS if not (root / "01_daily").exists() else root / "01_daily" / "news"
    # NEWS is already 01_daily/news; root / NEWS doubles if root is repo.
    news = root / "01_daily" / "news"
    exports = root / "data" / "exports"
    elite = root / "data" / "theme_radar_snapshots"
    if news.is_dir():
        for path in sorted(news.glob("*_parsed.json")):
            try:
                blob = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            for item in blob.get("all_items") or []:
                if isinstance(item, dict):
                    _add(
                        bag, str(item.get("title") or ""),
                        str(item.get("source") or item.get("summary") or ""),
                        str(path.relative_to(root)) if path.is_relative_to(root) else str(path),
                        str(item.get("published_at") or ""),
                        "", "parsed",
                    )
        for path in sorted(news.glob("*_finviz_digest.json")):
            try:
                blob = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            for item in (blob.get("top_signal") or []) + (blob.get("index_digests") or []):
                if isinstance(item, dict):
                    _add(
                        bag,
                        str(item.get("news_title") or item.get("digest") or item.get("title") or ""),
                        str(item.get("digest") or ""),
                        str(path), "", "", "finviz_digest",
                    )
    latest = None
    if exports.is_dir():
        dated = []
        for path in exports.glob("finviz_20*.csv"):
            m = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
            if m:
                dated.append((m.group(1), path))
        if dated:
            latest = sorted(dated)[-1][1]
            with latest.open(newline="", encoding="utf-8", errors="replace") as fh:
                for row in csv.DictReader(fh):
                    _add(
                        bag, str(row.get("News Title") or ""),
                        str(row.get("Daily Digest") or ""),
                        str(latest), str(row.get("News Time") or ""),
                        str(row.get("Ticker") or ""),
                        "finviz_export",
                    )
    if elite.is_dir():
        snaps = sorted(elite.glob("*.csv.gz"))
        if snaps:
            path = snaps[-1]
            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
                for row in csv.DictReader(fh):
                    _add(
                        bag, str(row.get("News Title") or ""),
                        str(row.get("Daily Digest") or ""),
                        str(path), str(row.get("News Time") or ""),
                        str(row.get("Ticker") or ""),
                        "elite_snapshot",
                    )
    return list(bag.values())


def _shard_of(title: str, shards: int) -> int:
    if shards <= 1:
        return 0
    return zlib.adler32(title.encode("utf-8")) % shards


# Analyst may use 8B only after event_class is locked. Classify never does.
ANALYST_LANES = [
    "openclaw",
    "zhipu", "openrouter", "qwen", "tokenhub",
    "siliconflow", "mistral", "nvidia_nim", "pollinations",
]
# Lookup core-vs-tangent filter. OpenClaw is first after class is locked.
# 8B remains the fallback when the gateway times out or rejects the JSON.
FILTER_LANES = [
    "openclaw",
    "siliconflow", "mistral", "openrouter", "qwen",
    "nvidia_nim", "pollinations",
]
_FLOOR_PROVIDERS = frozenset({
    "openclaw",
    "zhipu", "siliconflow", "openrouter", "qwen", "tokenhub", "gemini",
    "mistral", "pollinations",
})


def classify_lanes() -> list[str]:
    """Floor lanes. Ministral 8B/3B and NVIDIA NIM are not on this list."""
    return list(lane.CLASSIFY_LANES)


def classify_models_for(hop: str) -> list[str]:
    return lane.primary_models_for(hop, "news_classify")


def _json_shape(parsed) -> str:
    """Short reject detail for the gold log. No prompt body."""
    if not isinstance(parsed, dict):
        return " shape=empty" if parsed is None else " shape=not-object"
    bits = []
    if parsed.get("event_class") or parsed.get("q5"):
        bits.append(f"class={parsed.get('event_class')} q5={parsed.get('q5')}")
    if "core" in parsed or "tickers" in parsed:
        bits.append(f"core={parsed.get('core') or parsed.get('tickers')}")
    ents = parsed.get("entities")
    if isinstance(ents, list) and ents:
        ticks = []
        for ent in ents[:6]:
            if isinstance(ent, dict):
                ticks.append(f"{ent.get('ticker')}:{ent.get('direction')}")
        if ticks:
            bits.append("entities=" + ",".join(ticks))
    return (" " + " ".join(bits))[:180] if bits else " shape=object"


def _stage_repair(stage: str, prompt: str, parsed) -> str:
    if stage == "filter":
        return filter_repair_note(parsed, prompt)
    if stage == "analyst":
        return analyst_repair_note(parsed, prompt)
    return ""


class LiveLane:
    def __init__(self) -> None:
        try:
            from src.config import align_openclaw_token
            align_openclaw_token()
        except Exception as exc:
            print(f"[lane_one_shot] openclaw align skipped {str(exc)[:120]}")
        keys, ollama_url, gh_direct = lane.load_keys()
        self.ctx = {"keys": keys, "ollama_url": ollama_url, "gh_direct": gh_direct}
        lane._SKIP.clear()
        lane._RATE_LIMITED.clear()
        lane._MODEL_DENIED.clear()
        lane._QWEN_STANDING_HITS = 0
        lane._OR_DAY_CAPPED = False
        self.last_classify_note = ""
        self._rejected_classify = None

    def __call__(self, stage: str, prompt: str, system: str, accept=None):
        if stage == "classify":
            return self._classify(prompt, system, accept)
        # What to ask, and whether the pack is complete, use the classify floor.
        # 8B still filters Finviz core-vs-tangent and writes the analyst JSON.
        if stage in {"meta", "pack_complete"}:
            return self._floor(stage, prompt, system, accept)
        if stage in {"filter", "planner"}:
            return self._hop(stage, prompt, system, accept, FILTER_LANES, "news_filter")
        return self._hop(stage, prompt, system, accept, ANALYST_LANES, "news_impact")

    def _floor(self, stage: str, prompt: str, system: str, accept):
        """Zhipu / DashScope / TokenHub / OR gemma floor. Not 8B."""
        notes: list[str] = []
        zhipu_on = bool(self.ctx["keys"].get("zhipu"))
        budget = max(lane.token_budget("news_classify"), 1200)
        winner = None
        for hop in classify_lanes():
            models = classify_models_for(hop)
            if not models:
                note = (
                    f"{hop}: skipped for {stage} — allowlist is 8B-only, "
                    "and 8B is not a context floor"
                    if hop == "siliconflow"
                    else f"{hop}: no context-floor model"
                )
                notes.append(note)
                print(f"[lane_one_shot] {note}")
                continue
            if hop not in self.ctx["keys"]:
                note = f"{hop}: key missing"
                notes.append(note)
                print(f"[lane_one_shot] {note}")
                continue
            parsed, model = lane.ask_lane(
                hop, prompt, self.ctx,
                max_tokens=budget, system=system, tmpl="news_classify",
                accept=accept,
            )
            if parsed is None or lane.is_classify_banned(str(model or "")):
                notes.append(self._fail_note(hop, model))
                continue
            if accept is not None and not accept(parsed):
                note = f"{hop}/{model}: {stage} JSON rejected"
                notes.append(note)
                print(f"[lane_one_shot] {note}")
                time.sleep(0.2)
                continue
            print(f"[lane_one_shot] {stage} lane::{hop}::{model}")
            winner = (parsed, hop, str(model))
            break
        if winner is None:
            note = "; ".join(notes) or f"{stage} floor exhausted"
            if zhipu_on:
                print(
                    f"[lane_one_shot] {stage} stopped — ZHIPU key present, "
                    f"no floor watermark. {note}"
                )
            else:
                print(f"[lane_one_shot] {stage} floor exhausted. {note}")
            return None, "", ""
        _parsed, hop, model = winner
        if zhipu_on and hop != "zhipu":
            print(
                f"[lane_one_shot] {stage} watermark is not zhipu "
                f"while ZHIPU key is present: {'; '.join(notes)}"
            )
        time.sleep(0.4)
        return winner

    def _classify(self, prompt: str, system: str, accept):
        """Floor models only. If they all 429 or reject the enum, stop."""
        notes: list[str] = []
        zhipu_on = bool(self.ctx["keys"].get("zhipu"))
        # Floor models that think before the JSON need more than the 400
        # inbox budget. The global news_classify budget stays 400.
        budget = max(lane.token_budget("news_classify"), 900)
        winner = None
        for hop in classify_lanes():
            models = classify_models_for(hop)
            if not models:
                note = (
                    f"{hop}: skipped for classify — allowlist is 8B-only, "
                    "and 8B is not a classify floor"
                    if hop == "siliconflow"
                    else f"{hop}: no classify-floor model"
                )
                notes.append(note)
                print(f"[lane_one_shot] {note}")
                continue
            if hop not in self.ctx["keys"]:
                note = f"{hop}: key missing"
                notes.append(note)
                print(f"[lane_one_shot] {note}")
                continue
            parsed, model = self._ask_classify(
                hop, prompt, system, accept, budget,
            )
            if (
                hop == "openclaw"
                and (parsed is None or lane.is_classify_banned(str(model or "")))
                and accept is not None
            ):
                rejected = self._rejected_classify
                repair = classify_repair_note(rejected, prompt, "")
                if repair:
                    print(
                        "[lane_one_shot] openclaw classify near-miss "
                        "— one repair before the hopper"
                    )
                    parsed, model = self._ask_classify(
                        hop, prompt + "\n\n" + repair, system, accept, budget,
                        capture=False,
                    )
                # OpenClaw returned an enum and the floor rejected it.
                # Do not let a later hopper ID lock the class.
                if self._rejected_classify is not None and (
                    parsed is None
                    or lane.is_classify_banned(str(model or ""))
                    or not accept(parsed)
                ):
                    note = (
                        "openclaw: pinned floor rejected the enum "
                        "— hopper not used"
                    )
                    notes.append(note)
                    print(f"[lane_one_shot] {note}")
                    break
            if parsed is None or lane.is_classify_banned(str(model or "")):
                notes.append(self._fail_note(hop, model))
                continue
            if accept is not None and not accept(parsed):
                note = (
                    f"{hop}/{model}: JSON rejected "
                    "(bad enum or regime_break dump-bucket)"
                )
                notes.append(note)
                print(f"[lane_one_shot] {note}")
                time.sleep(0.2)
                continue
            print(f"[lane_one_shot] classify lane::{hop}::{model}")
            winner = (parsed, hop, str(model))
            break
        if winner is None:
            self.last_classify_note = "; ".join(notes) or "classify floor exhausted"
            if zhipu_on:
                print(
                    "[lane_one_shot] classify stopped — ZHIPU key present, "
                    f"no floor watermark. {self.last_classify_note}"
                )
            else:
                print(f"[lane_one_shot] classify floor exhausted. {self.last_classify_note}")
            return None, "", ""
        _parsed, hop, model = winner
        if zhipu_on and hop != "zhipu":
            self.last_classify_note = "; ".join(notes) or "zhipu did not lock the class"
            print(
                "[lane_one_shot] classify watermark is not zhipu "
                f"while ZHIPU key is present: {self.last_classify_note}"
            )
        else:
            self.last_classify_note = ""
        time.sleep(0.4)
        return winner

    def _ask_classify(self, hop, prompt, system, accept, budget, capture=True):
        """One classify hop. A rejected JSON is kept for the OpenClaw repair."""
        rejected = {}

        def _watch(blob, _accept=accept):
            ok = _accept(blob) if _accept is not None else True
            if capture and not ok and isinstance(blob, dict):
                rejected["blob"] = blob
            return ok

        parsed, model = lane.ask_lane(
            hop, prompt, self.ctx,
            max_tokens=budget, system=system, tmpl="news_classify",
            accept=_watch if accept is not None else None,
        )
        if capture:
            self._rejected_classify = rejected.get("blob")
        return parsed, model

    def _fail_note(self, hop: str, model) -> str:
        if hop == "qwen" and lane._QWEN_STANDING_HITS >= lane._QWEN_STANDING_STOP:
            note = f"{hop}: standing arrearage — list short-circuited, provider kept"
        elif hop in lane._SKIP:
            note = f"{hop}: provider skipped (401/403/402/410) after a hard fail"
        elif any(str(k).startswith(f"{hop}::") for k in lane._RATE_LIMITED):
            note = f"{hop}: 429 on {model or 'floor model'} — next ID, provider kept"
        else:
            note = f"{hop}: no usable classify JSON ({model or 'no model'})"
        print(f"[lane_one_shot] {note}")
        return note

    def _hop(self, stage, prompt, system, accept, hops, tmpl):
        budget = max(lane.token_budget(tmpl), 900)
        for hop in hops:
            parsed, model = lane.ask_lane(
                hop, prompt, self.ctx,
                max_tokens=budget, system=system, tmpl=tmpl,
            )
            if parsed is None:
                continue
            if lane.is_banned_primary(str(model or "")):
                print(f"[lane_one_shot] skip banned {hop}/{model}")
                continue
            if stage == "classify" and lane.is_classify_banned(str(model or "")):
                print(f"[lane_one_shot] skip below-floor {hop}/{model}")
                continue
            if accept is not None and not accept(parsed):
                print(
                    f"[lane_one_shot] {stage} unusable lane::{hop}::{model}"
                    f"{_json_shape(parsed)}"
                )
                if hop == "openclaw":
                    note = _stage_repair(stage, prompt, parsed)
                    if note:
                        print(
                            f"[lane_one_shot] openclaw {stage} near-miss "
                            "— one repair before the hopper"
                        )
                        parsed2, model2 = lane.ask_lane(
                            hop, prompt + "\n\n" + note, self.ctx,
                            max_tokens=budget, system=system, tmpl=tmpl,
                        )
                        if (
                            parsed2 is not None
                            and not lane.is_banned_primary(str(model2 or ""))
                            and accept(parsed2)
                        ):
                            print(f"[lane_one_shot] {stage} lane::{hop}::{model2}")
                            time.sleep(0.4)
                            return parsed2, hop, str(model2)
                        print(
                            f"[lane_one_shot] {stage} repair unusable "
                            f"lane::{hop}::{model2}{_json_shape(parsed2)}"
                        )
                time.sleep(0.2)
                continue
            print(f"[lane_one_shot] {stage} lane::{hop}::{model}")
            time.sleep(0.4)
            return parsed, hop, str(model)
        print(f"[lane_one_shot] {stage} hops failed")
        return None, "", ""


def _save_scratch(row: dict, folder: Path) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{row['article_id']}.json"
    path.write_text(json.dumps(row, indent=2, ensure_ascii=False), encoding="utf-8")


def _valid_watermark(row: dict) -> bool:
    marks = [row.get("watermark") or ""]
    marks += [w.get("watermark") or "" for w in row.get("watermarks") or []]
    if not marks or not all(_WM.match(m) for m in marks if m):
        return False
    if not _WM.match(row.get("watermark") or ""):
        return False
    for mark in marks:
        model = mark.split("::", 2)[-1] if mark else ""
        if model and lane.is_banned_primary(model):
            return False
    return True


def _context_floor_ok(row: dict) -> bool:
    """Meta and pack-complete watermarks must be floor models, not 8B."""
    stages = {}
    for mark in row.get("watermarks") or []:
        if mark.get("stage") in {"meta", "pack_complete"}:
            stages[mark.get("stage")] = str(mark.get("watermark") or "")
    if "meta" not in stages or "pack_complete" not in stages:
        return False
    for wm in stages.values():
        parts = wm.split("::")
        if len(parts) < 3 or parts[1] not in _FLOOR_PROVIDERS:
            return False
        if lane.is_classify_banned(parts[-1]):
            return False
    return True


def _classify_floor_ok(row: dict) -> bool:
    """Classify watermark must be a floor model. Ministral cannot lock class."""
    for mark in row.get("watermarks") or []:
        if mark.get("stage") != "classify":
            continue
        parts = str(mark.get("watermark") or "").split("::")
        if len(parts) < 3 or parts[1] not in _FLOOR_PROVIDERS:
            return False
        return not lane.is_classify_banned(parts[-1])
    return False


# 18:30 ET -> 03:30 ET. Shards share the one ECS runner, so this is the
# whole run (gold + five shards + merge), not a per-shard allowance.
DEFAULT_RUN_BUDGET_MINUTES = 540
DEFAULT_DEADLINE_ET = "03:30"
# Gold fixtures on run 35929449213 took 54 minutes. 75 leaves slack
# for checkout without giving gold the old 120-minute job cap.
DEFAULT_GOLD_BUDGET_MINUTES = 75
DEFAULT_MERGE_BUDGET_MINUTES = 20
# Checkout, install, env check, and artifact upload sit inside the
# GitHub job timeout. The draw loop stops this early so the upload
# step still runs.
JOB_EDGE_SLACK_SECONDS = 8 * 60
# Upload itself, after the interpreter returns, before merge starts.
UPLOAD_TAIL_SECONDS = 3 * 60
_HHMM = re.compile(r"^(\d{1,2}):(\d{2})$")
_PARTIAL_STOPS = frozenset({
    "budget", "deadline", "max_draws", "pool_exhausted", "running",
})


class BudgetInterrupted(BaseException):
    """SIGTERM while a hop is in flight. Not an Exception, so HTTP handlers
    that swallow Exception still let the draw loop flush the shard file."""


def _raise_budget(signum, frame):
    raise BudgetInterrupted()


def parse_deadline_et(text: str) -> tuple[int, int]:
    match = _HHMM.match((text or "").strip())
    if not match:
        raise SystemExit(f"[lane_one_shot] deadline_et must be HH:MM, got {text!r}")
    hour, minute = int(match.group(1)), int(match.group(2))
    if hour > 23 or minute > 59:
        raise SystemExit(f"[lane_one_shot] deadline_et out of range: {text}")
    return hour, minute


def next_deadline_epoch(now: datetime, deadline_et: str) -> int:
    """Next America/New_York clock time matching deadline_et, strictly after now."""
    hour, minute = parse_deadline_et(deadline_et)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    local = now.astimezone(ZoneInfo("America/New_York"))
    candidate = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= local:
        candidate = candidate + timedelta(days=1)
    return int(candidate.timestamp())


def plan_run_budgets(
    now: datetime | None = None,
    *,
    run_budget_minutes: float = DEFAULT_RUN_BUDGET_MINUTES,
    deadline_et: str = DEFAULT_DEADLINE_ET,
    shards: int = 5,
    gold_budget_minutes: float = DEFAULT_GOLD_BUDGET_MINUTES,
    merge_budget_minutes: float = DEFAULT_MERGE_BUDGET_MINUTES,
) -> dict:
    """Split one overnight window across gold, serial shards, and merge.

    Each shard gets (window - gold - merge) / shards. The window is the
    earlier of now+run_budget_minutes and the next deadline_et.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    shards = max(1, int(shards))
    now_epoch = int(now.timestamp())
    deadline_epoch = next_deadline_epoch(now, deadline_et)
    budget_end = now_epoch + int(float(run_budget_minutes) * 60)
    effective = min(deadline_epoch, budget_end)
    window = max(0, effective - now_epoch)
    merge_seconds = max(0, int(float(merge_budget_minutes) * 60))
    gold_cap = max(0, int(float(gold_budget_minutes) * 60))
    gold_seconds = min(gold_cap, max(0, window - merge_seconds))
    rest = max(0, window - gold_seconds - merge_seconds)
    per_shard = rest // shards
    return {
        "deadline_epoch": effective,
        "deadline_et": (deadline_et or DEFAULT_DEADLINE_ET).strip(),
        "window_seconds": window,
        "gold_timeout_minutes": max(1, gold_seconds // 60),
        "gold_budget_seconds": max(0, gold_seconds - JOB_EDGE_SLACK_SECONDS),
        "merge_timeout_minutes": max(1, merge_seconds // 60),
        "merge_reserve_seconds": merge_seconds,
        "shard_job_timeout_minutes": max(1, per_shard // 60),
        "shard_budget_seconds": max(0, per_shard - JOB_EDGE_SLACK_SECONDS),
        "shards": shards,
    }


def emit_budget_plan(plan: dict) -> None:
    """Print the plan and append KEY=VALUE lines to GITHUB_OUTPUT when set."""
    keys = (
        "deadline_epoch",
        "gold_timeout_minutes",
        "gold_budget_seconds",
        "merge_timeout_minutes",
        "merge_reserve_seconds",
        "shard_job_timeout_minutes",
        "shard_budget_seconds",
        "shards",
        "window_seconds",
    )
    lines = [f"{key}={int(plan[key])}" for key in keys]
    text = "\n".join(lines) + "\n"
    print("[lane_one_shot] budget plan")
    for line in lines:
        print(" ", line)
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(text)


def _coverage_from(blob: dict | None, index: int, present: bool) -> dict:
    if not present or blob is None:
        return {
            "shard": index,
            "status": "missing",
            "n_kept": 0,
            "n_drawn": 0,
            "n_rejected": 0,
            "target": "",
            "partial": True,
            "stop_reason": "missing",
            "elapsed_seconds": None,
        }
    stop = str(blob.get("stop_reason") or "")
    partial = bool(blob.get("partial")) or stop in _PARTIAL_STOPS or stop == "missing"
    if not stop:
        partial = partial or int(blob.get("n_kept") or 0) < int(blob.get("target") or 0)
        stop = "partial" if partial else "complete"
    return {
        "shard": int(blob.get("shard") if blob.get("shard") is not None else index),
        "status": "partial" if partial else "complete",
        "n_kept": int(blob.get("n_kept") or 0),
        "n_drawn": int(blob.get("n_drawn") or 0),
        "n_rejected": int(blob.get("n_rejected") or 0),
        "target": blob.get("target") if blob.get("target") is not None else "",
        "partial": partial,
        "stop_reason": stop,
        "elapsed_seconds": blob.get("elapsed_seconds"),
    }


def run_shard(
    shard: int,
    shards: int,
    target: int,
    max_draws: int,
    *,
    root: Path | None = None,
    lane_client=None,
    use_pack: bool = True,
    gold_only: bool = False,
    budget_seconds: float | None = None,
    deadline_epoch: float | None = None,
    merge_reserve_seconds: float = 0,
    clock=None,
) -> dict:
    root = root or Path(".")
    ready = print_env()
    if lane_client is None and not ready:
        print("[lane_one_shot] no hopper secrets — refusing to write a board")
        raise SystemExit(2)
    clock = clock or time.time
    started = clock()
    capped_by_deadline = False
    hard_stop = None
    if deadline_epoch:
        hard_stop = (
            float(deadline_epoch)
            - float(merge_reserve_seconds or 0)
            - UPLOAD_TAIL_SECONDS
        )
        remain = hard_stop - started
        if budget_seconds is None or remain < float(budget_seconds):
            budget_seconds = max(0.0, remain)
            capped_by_deadline = True
    elif budget_seconds is not None:
        budget_seconds = max(0.0, float(budget_seconds))
    client = lane_client or LiveLane()
    axioms = load_axioms()
    index = get_index(root)

    def pack_fn(query, body=""):
        from src.news_impact.search_pack import overview_first
        return overview_first(query or body)

    scratch = root / SCRATCH
    kept: list[dict] = []
    rejected: list[dict] = []
    drawn = 0
    gold_report: dict[str, str] = {}
    gold_rows: list[dict] = []

    def handle(art: dict) -> None:
        nonlocal drawn
        drawn += 1
        lane.release_transient_limits()
        row = process_article(
            art, client, axioms=axioms, use_pack=use_pack,
            pack_fn=pack_fn if use_pack else None,
            root=root, index_names=index.title_names,
        )
        if row.get("gold_id"):
            gold_rows.append(row)
        if row.get("gold_id") and gate0(art.get("title") or "", art.get("body") or ""):
            gold_report[row["gold_id"]] = "REJECTED"
        elif row.get("gold_id"):
            gold_report[row["gold_id"]] = row.get("gold_status") or (
                "PASS" if row.get("keep") else "FAIL"
            )
        if (
            row.get("keep")
            and _valid_watermark(row)
            and _classify_floor_ok(row)
            and _context_floor_ok(row)
        ):
            kept.append(row)
            _save_scratch(row, scratch)
            print(f"[lane_one_shot] KEEP {row.get('watermark')} {row.get('action','')[:120]}")
        else:
            rejected.append({
                "title": row.get("title"),
                "reason": row.get("reject_reason") or "drop",
                "gold_id": row.get("gold_id") or "",
                "watermark": row.get("watermark") or "",
            })
            if row.get("watermarks"):
                _save_scratch(row, scratch)
            if row.get("keep") and not _classify_floor_ok(row):
                print(f"[lane_one_shot] classify below floor {row.get('watermark')}")
            print(f"[lane_one_shot] REJECT {row.get('reject_reason')} {(row.get('title') or '')[:80]}")

    stop_reason = ""

    def _expired() -> str:
        if budget_seconds is None:
            return ""
        if (clock() - started) >= float(budget_seconds):
            return "deadline" if capped_by_deadline else "budget"
        return ""

    def _report(tape: dict) -> dict:
        invented = 0
        hops: Counter = Counter()
        classify_hops: Counter = Counter()
        reasons: Counter = Counter()
        for row in kept:
            invented += len(row.get("invented_tickers") or [])
            for w in row.get("watermarks") or []:
                if w.get("watermark"):
                    hops[w["watermark"]] += 1
                    if w.get("stage") == "classify":
                        classify_hops[w["watermark"]] += 1
        for row in rejected:
            reasons[row.get("reason") or "?"] += 1
        reason = stop_reason or "running"
        partial = reason in _PARTIAL_STOPS
        elapsed = max(0, int(clock() - started))
        return {
            "shard": shard,
            "shards": shards,
            "target": target,
            "partial": partial,
            "stop_reason": reason,
            "elapsed_seconds": elapsed,
            "budget_seconds": None if budget_seconds is None else int(budget_seconds),
            "n_drawn": drawn,
            "n_rejected": len(rejected),
            "n_kept": len(kept),
            "invented_tickers": invented,
            "hop_histogram": dict(hops),
            "classify_histogram": dict(classify_hops),
            "reject_histogram": dict(reasons),
            "gold": gold_report,
            "gold_rows": gold_rows,
            "tape": tape,
            "env": env_lines(),
            "finviz_file": index.source,
            "kept": kept,
            "rejected": rejected[:80],
        }

    def _flush(tape: dict | None = None) -> dict:
        blob = _report({} if tape is None else tape)
        scratch.mkdir(parents=True, exist_ok=True)
        (scratch / f"shard_{shard}.json").write_text(
            json.dumps(blob, indent=2, ensure_ascii=False), encoding="utf-8",
        )
        return blob

    previous_term = None
    if budget_seconds is not None or deadline_epoch:
        previous_term = signal.signal(signal.SIGTERM, _raise_budget)
    tape: dict = {}
    try:
        if shard == 0 or gold_only:
            for art in GOLD_KEEP + GOLD_REJECT + GOLD_EXTRA:
                if len(kept) >= target and not gold_only:
                    stop_reason = "target"
                    break
                reason = _expired()
                if reason:
                    stop_reason = reason
                    break
                try:
                    handle(art)
                except BudgetInterrupted:
                    stop_reason = "deadline" if capped_by_deadline else "budget"
                    break
                _flush()
        if not stop_reason and not gold_only:
            pool = [
                art for art in harvest(root)
                if _shard_of(art["title"], shards) == shard
                and _norm(art["title"]) not in {
                    _norm(g["title"]) for g in GOLD_KEEP + GOLD_REJECT + GOLD_EXTRA
                }
            ]
            pool.sort(key=lambda a: a["title"])
            reason = _expired()
            if reason:
                stop_reason = reason
                pool = []
            for art in pool:
                if len(kept) >= target:
                    stop_reason = "target"
                    break
                reason = _expired()
                if reason:
                    stop_reason = reason
                    break
                if drawn >= max_draws:
                    stop_reason = "max_draws"
                    break
                try:
                    handle(art)
                except BudgetInterrupted:
                    stop_reason = "deadline" if capped_by_deadline else "budget"
                    break
                _flush()
        if not stop_reason:
            if gold_only:
                stop_reason = "gold_done"
            elif len(kept) >= target:
                stop_reason = "target"
            elif drawn >= max_draws:
                stop_reason = "max_draws"
            else:
                stop_reason = "pool_exhausted"
        fetch_tape = stop_reason not in {"budget", "deadline"}
        try:
            tape = grade_action_rows(kept, fetch=fetch_tape)
        except BudgetInterrupted:
            stop_reason = "deadline" if capped_by_deadline else "budget"
            tape = {}
        except Exception as exc:  # noqa: BLE001
            print(f"[lane_one_shot] tape grade failed {str(exc)[:160]}")
            try:
                tape = grade_action_rows(kept, fetch=False)
            except Exception:
                tape = {}
    except BudgetInterrupted:
        if not stop_reason:
            stop_reason = "deadline" if capped_by_deadline else "budget"
        tape = {}
    finally:
        if previous_term is not None:
            signal.signal(signal.SIGTERM, previous_term)
    report = _flush(tape)
    print(
        f"[lane_one_shot] shard {shard} kept={len(kept)} "
        f"drawn={drawn} rejected={len(rejected)} "
        f"stop={report.get('stop_reason')} partial={report.get('partial')} "
        f"elapsed={report.get('elapsed_seconds')}s"
    )
    return report


def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def merge_boards(
    src: Path,
    board: Path | None = None,
    expect: int = 100,
    shard_count: int | None = None,
) -> int:
    shards = sorted(src.rglob("shard_*.json"))
    if not shards and (src / "shard_0.json").is_file():
        shards = sorted(src.glob("shard_*.json"))
    if not shards:
        print("[lane_one_shot] no shard files — not writing a board")
        return 2
    kept: list[dict] = []
    drawn = rejected = invented = 0
    hops: Counter = Counter()
    classify_hops: Counter = Counter()
    reasons: Counter = Counter()
    gold: dict[str, str] = {}
    env: list[str] = []
    finviz_file = ""
    found: dict[int, dict] = {}
    for path in shards:
        blob = _load_json(path)
        match = re.search(r"shard_(\d+)\.json$", path.name)
        if match:
            found[int(match.group(1))] = blob
        drawn += int(blob.get("n_drawn") or 0)
        rejected += int(blob.get("n_rejected") or 0)
        invented += int(blob.get("invented_tickers") or 0)
        hops.update(blob.get("hop_histogram") or {})
        classify_hops.update(blob.get("classify_histogram") or {})
        reasons.update(blob.get("reject_histogram") or {})
        gold.update(blob.get("gold") or {})
        env = blob.get("env") or env
        finviz_file = blob.get("finviz_file") or finviz_file
        for row in blob.get("kept") or []:
            if row.get("keep") and _valid_watermark(row) and row.get("action"):
                if "no action" in str(row.get("action")).lower():
                    continue
                if not _classify_floor_ok(row) or not _context_floor_ok(row):
                    continue
                kept.append(row)
    # Stable order: gold keepers first, then the rest.
    gold_order = {g["gold_id"]: i for i, g in enumerate(GOLD_KEEP)}

    def sort_key(row: dict):
        gid = row.get("gold_id") or ""
        return (0 if gid in gold_order else 1, gold_order.get(gid, 99), row.get("title") or "")

    kept.sort(key=sort_key)
    # De-dupe by article id.
    seen, uniq = set(), []
    for row in kept:
        if row.get("article_id") in seen:
            continue
        seen.add(row["article_id"])
        uniq.append(row)
    kept = uniq[:expect]
    if shard_count is None:
        shard_count = (max(found) + 1) if found else 0
    coverage = [
        _coverage_from(found.get(i), i, i in found) for i in range(int(shard_count))
    ]
    any_partial = any(item.get("status") != "complete" for item in coverage)
    if invented:
        status = "REFUSED_INVENTED"
    elif len(kept) >= expect and not any_partial:
        status = "OK"
    elif any_partial:
        status = "PARTIAL"
    else:
        status = "SHORTFALL"
    header = {
        "n_drawn": drawn,
        "n_rejected": rejected,
        "n_kept": len(kept),
        "invented_tickers": invented,
        "hop_histogram": dict(hops.most_common()),
        "classify_histogram": dict(classify_hops.most_common()),
        "reject_histogram": dict(reasons.most_common()),
        "gold": gold,
        "env": env,
        "finviz_file": finviz_file,
        "status": status,
        "shard_coverage": coverage,
        "tape": summarize_tape(kept),
    }
    dest = board or BOARD
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(render_markdown(header, kept), encoding="utf-8")
    # Scoreboard merges land scratch in the repo. Any other board (tests)
    # keeps its scratch beside the board so a tmp path cannot touch the checkout.
    if "03_scoreboard" in dest.parts:
        out_scratch = dest.parent.parent / "02_lessons" / "lane" / "one_shot_100"
    else:
        out_scratch = dest.parent / "one_shot_100"
    out_scratch.mkdir(parents=True, exist_ok=True)
    for row in kept:
        (out_scratch / f"{row['article_id']}.json").write_text(
            json.dumps(row, indent=2, ensure_ascii=False), encoding="utf-8",
        )
    print(f"[lane_one_shot] wrote {dest} kept={len(kept)} status={status}")
    for item in coverage:
        print(
            f"[lane_one_shot] coverage shard {item.get('shard')} "
            f"{item.get('status')} kept={item.get('n_kept')} "
            f"stop={item.get('stop_reason')}"
        )
    if invented:
        print("[lane_one_shot] refusing invented tickers", invented)
        return 2
    return 0


def assess_board(text: str) -> list[str]:
    """Problems that keep the published board from being merge-ready."""
    problems = []

    def grab(key: str) -> str:
        match = re.search(rf"^- {re.escape(key)}: (.*)$", text, re.M)
        return match.group(1).strip() if match else ""

    try:
        n_kept = int(grab("n_kept") or "0")
    except ValueError:
        n_kept = 0
    try:
        invented = int(grab("invented_tickers") or "0")
    except ValueError:
        invented = 1
    if n_kept < 100:
        problems.append(f"n_kept={n_kept}")
    if invented:
        problems.append(f"invented_tickers={invented}")
    if grab("status") != "OK":
        problems.append(f"status={grab('status') or 'missing'}")
    gold_block = ""
    if "## Gold fixtures" in text:
        gold_block = text.split("## Gold fixtures", 1)[1].split("## ", 1)[0]
    expected = {
        "tsa": "PASS",
        "buist": "PASS",
        "tsv": "PASS",
        "amrx": "PASS",
        "naion": "PASS",
        "hormuz": "REJECTED",
        "outperforms": "REJECTED",
    }
    for key, want in expected.items():
        match = re.search(rf"^- {key}: (\S+)", gold_block, re.M)
        got = match.group(1) if match else "missing"
        if got != want:
            problems.append(f"gold {key}={got}")
    classify_block = ""
    if "## Classify hop histogram" in text:
        classify_block = text.split("## Classify hop histogram", 1)[1].split("## ", 1)[0]
    else:
        problems.append("classify histogram missing")
    if "ministral" in classify_block.lower():
        problems.append("ministral on classify")
    floor = (
        "lane::zhipu::", "lane::openrouter::", "lane::qwen::",
        "lane::tokenhub::", "lane::gemini::",
        "lane::mistral::", "lane::pollinations::",
        "lane::openclaw::",
    )
    if not any(prefix in classify_block for prefix in floor):
        problems.append("classify histogram has no floor model")
    if "no action warranted" in text.lower():
        problems.append("banned phrase")
    body = text.split("## Rows", 1)[-1] if "## Rows" in text else ""
    if "### " in body:
        if "M1:" not in body:
            problems.append("meta section missing")
        if "history_state:" not in body:
            problems.append("history section missing")
        if "native clock:" not in body:
            problems.append("tape columns missing")
    return problems


def gold_gate_problems(report: dict) -> list[str]:
    """Gold-only gate. The four keepers must pass before any 100-draw."""
    problems = []
    gold = report.get("gold") or {}
    for key in ("tsa", "buist", "tsv", "amrx", "naion"):
        if gold.get(key) != "PASS":
            problems.append(f"gold {key}={gold.get(key) or 'missing'}")
    for key in ("hormuz", "outperforms"):
        if gold.get(key) != "REJECTED":
            problems.append(f"gold {key}={gold.get(key) or 'missing'}")
    hist = report.get("classify_histogram") or {}
    if any("ministral" in str(key).lower() for key in hist):
        problems.append("ministral on classify")
    for row in report.get("kept") or []:
        if row.get("gold_id") and not _classify_floor_ok(row):
            problems.append(f"classify below floor {row.get('gold_id')}")
        if row.get("gold_id") and not _context_floor_ok(row):
            problems.append(f"context below floor {row.get('gold_id')}")
    return problems


def write_gold_report(report: dict) -> int:
    problems = gold_gate_problems(report)
    header = {
        "n_drawn": report.get("n_drawn"),
        "n_rejected": report.get("n_rejected"),
        "n_kept": report.get("n_kept"),
        "invented_tickers": report.get("invented_tickers") or 0,
        "hop_histogram": report.get("hop_histogram") or {},
        "classify_histogram": report.get("classify_histogram") or {},
        "reject_histogram": report.get("reject_histogram") or {},
        "gold": report.get("gold") or {},
        "env": report.get("env") or [],
        "finviz_file": report.get("finviz_file") or "",
        "status": "GOLD_PASS" if not problems else "GOLD_FAIL",
    }
    dest = Path("03_scoreboard/LANE_ONE_SHOT_GOLD.md")
    dest.parent.mkdir(parents=True, exist_ok=True)
    rows = report.get("gold_rows") or [
        row for row in report.get("kept") or [] if row.get("gold_id")
    ]
    missing = [row for row in rows if row.get("action") and "tape" not in row]
    if missing:
        try:
            grade_action_rows(missing, fetch=True)
        except Exception as exc:  # noqa: BLE001 — a tape miss is unscored, not a class
            print(f"[lane_one_shot] tape grade failed {str(exc)[:160]}")
            grade_action_rows(missing, fetch=False)
    header["tape"] = summarize_tape(rows)
    dest.write_text(render_markdown(header, rows), encoding="utf-8")
    print(f"[lane_one_shot] wrote {dest} status={header['status']}")
    for item in problems:
        print("[lane_one_shot] gold gate:", item)
    return 0 if not problems else 1


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-check", action="store_true")
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--target", type=int, default=100)
    ap.add_argument("--max-draws", type=int, default=400)
    ap.add_argument("--merge", type=str, default="")
    ap.add_argument("--expect", type=int, default=100)
    ap.add_argument("--check-board", type=str, default="")
    ap.add_argument("--gold-only", action="store_true")
    ap.add_argument("--no-pack", action="store_true")
    ap.add_argument("--budget-seconds", type=float, default=None)
    ap.add_argument("--deadline-epoch", type=float, default=None)
    ap.add_argument("--merge-reserve-seconds", type=float, default=0)
    ap.add_argument("--print-budgets", action="store_true")
    ap.add_argument("--run-budget-minutes", type=float, default=DEFAULT_RUN_BUDGET_MINUTES)
    ap.add_argument("--deadline-et", default=DEFAULT_DEADLINE_ET)
    ap.add_argument("--gold-budget-minutes", type=float, default=DEFAULT_GOLD_BUDGET_MINUTES)
    ap.add_argument("--merge-budget-minutes", type=float, default=DEFAULT_MERGE_BUDGET_MINUTES)
    ap.add_argument("--shard-count", type=int, default=0)
    args = ap.parse_args()
    if args.print_budgets:
        emit_budget_plan(plan_run_budgets(
            run_budget_minutes=args.run_budget_minutes,
            deadline_et=args.deadline_et,
            shards=args.shards if args.shards > 1 else 5,
            gold_budget_minutes=args.gold_budget_minutes,
            merge_budget_minutes=args.merge_budget_minutes,
        ))
        raise SystemExit(0)
    if args.env_check:
        raise SystemExit(0 if print_env() and secrets_ready() else 2)
    if args.check_board:
        path = Path(args.check_board)
        if not path.is_file():
            print("[lane_one_shot] board missing", path)
            raise SystemExit(1)
        problems = assess_board(path.read_text(encoding="utf-8"))
        if problems:
            for item in problems:
                print("[lane_one_shot] not ready:", item)
            raise SystemExit(1)
        print("[lane_one_shot] board ready")
        raise SystemExit(0)
    if args.merge:
        count = args.shard_count or None
        raise SystemExit(merge_boards(
            Path(args.merge), expect=args.expect, shard_count=count,
        ))
    budget = args.budget_seconds
    deadline = args.deadline_epoch
    reserve = args.merge_reserve_seconds
    if args.gold_only:
        report = run_shard(
            0, 1, 7, 7, use_pack=not args.no_pack, gold_only=True,
            budget_seconds=budget,
            deadline_epoch=deadline,
            merge_reserve_seconds=reserve,
        )
        raise SystemExit(write_gold_report(report))
    run_shard(
        args.shard, args.shards, args.target, args.max_draws,
        use_pack=not args.no_pack,
        budget_seconds=budget,
        deadline_epoch=deadline,
        merge_reserve_seconds=reserve,
    )


if __name__ == "__main__":
    main()
