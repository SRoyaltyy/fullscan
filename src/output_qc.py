"""Self-read quality control for pre-open artifacts.

After every predictive write the runner MUST read the file back as text
and throw it out if it is:

  - empty / tiny
  - an OpenClaw/LLM timeout or idle-timeout stub (those used to land as
    0/flat because parse_scores defaults missing keys to 0.0)
  - a carry-forward from an older day
  - missing the contract markers the DeepSeek gold format requires

This module is the single source of truth. Runners (run_predict,
run_sector_predict, run_events, run_news_judge, …) and the orchestrator
both consult it. Never treat GitHub `conclusion=success` or file
existence as quality.

CLI:
  python -m src.output_qc --date YYYY-MM-DD --preopen
  python -m src.output_qc --kind sector --path 01_daily/sectors/DATE/slug_predict.md
"""
from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config
from .sector_taxonomy import FINVIZ_SECTORS

# Timeout / gateway-error / shutdown text that OpenClaw sometimes returns
# as the "assistant" content. Short stubs of this form previously became
# 0/flat. Infra-specific phrasing only — "government shutdown" in a real
# market essay must NOT trip this (looks_like_timeout additionally
# requires missing contract markers or a short body).
TIMEOUT_RE = re.compile(
    r"(LLM request timed out"
    r"|model idle timeout"
    r"|idle timeout"
    r"|gateway timeout"
    r"|prompt too long"
    r"|prompt is too long"
    r"|context length exceeded"
    r"|maximum context length"
    r"|The model did not produce a response"
    r"|gateway is shutting down"
    r"|server is shutting down"
    r"|instance is shutting down"
    r"|received a shutdown signal"
    r"|agent was aborted"
    r"|run was aborted"
    r"|connection refused"
    r"|ECONNREFUSED"
    r"|ECONNRESET"
    r"|502 Bad Gateway"
    r"|503 Service Unavailable"
    r"|upstream connect error)",
    re.I,
)

CARRIED_RE = re.compile(
    r"(CARRIED FORWARD|carried_from|status[\"']?\s*:\s*[\"']carried)",
    re.I,
)

MIN_PREDICT_CHARS = 2000
MIN_SECTOR_CHARS = 2500
MIN_JUDGE_CHARS = 400
MIN_EVENTS_CHARS = 200


@dataclass
class QCResult:
    ok: bool
    kind: str
    path: str
    reason: str = ""
    size: int = 0
    carried: bool = False
    timeout: bool = False
    empty: bool = False
    missing_markers: list[str] = field(default_factory=list)

    def explain(self) -> str:
        flag = "OK" if self.ok else "FAIL"
        extra = f" — {self.reason}" if self.reason else ""
        return f"[{flag}] {self.kind} {self.path} ({self.size} chars){extra}"


def _read(path: str | Path) -> str:
    try:
        return Path(path).read_text(encoding="utf-8")
    except OSError:
        return ""


def _read_json(path: str | Path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def looks_like_timeout(text: str) -> bool:
    """True when the body is a timeout/error stub, not a real essay.

    A long essay that happens to quote 'timed out' still passes: we only
    trip when the timeout phrase is present AND the text is short or
    missing every score/parse contract marker.
    """
    if not text or not text.strip():
        return False
    if not TIMEOUT_RE.search(text):
        return False
    has_contract = any(m in text for m in (
        "SCORES_BEGIN", "SECTOR_SCORES_BEGIN", "NEWS_PARSE_BEGIN",
        "HIT_GRID_BEGIN",
    ))
    return (not has_contract) or len(text.strip()) < 2500


def _missing(text: str, markers: list[str]) -> list[str]:
    return [m for m in markers if m not in text]


def _fail(kind: str, path: str, reason: str, text: str = "",
          carried: bool = False, timeout: bool = False,
          empty: bool = False, missing: list[str] | None = None) -> QCResult:
    return QCResult(
        ok=False, kind=kind, path=str(path), reason=reason,
        size=len(text or ""), carried=carried, timeout=timeout,
        empty=empty, missing_markers=list(missing or []),
    )


def _ok(kind: str, path: str, text: str) -> QCResult:
    return QCResult(ok=True, kind=kind, path=str(path), size=len(text or ""))


# ---------------------------------------------------------------------------
# Per-artifact checks
# ---------------------------------------------------------------------------

def qc_text_general_predict(text: str, path: str = "") -> QCResult:
    if not (text or "").strip():
        return _fail("general_predict", path, "empty", empty=True)
    if looks_like_timeout(text):
        return _fail("general_predict", path, "timeout_stub", text, timeout=True)
    if len(text) < MIN_PREDICT_CHARS:
        return _fail("general_predict", path, f"too_small({len(text)})", text,
                     empty=True)
    missing = _missing(text, ["MEMORY_CONFIRM", "SCORES_BEGIN", "SCORES_END"])
    if missing:
        return _fail("general_predict", path,
                     "missing:" + ",".join(missing), text, missing=missing)
    # Need real component lines, not an empty SCORES block.
    comps = re.findall(r"^B[0-9]_[A-Z0-9_]+:\s*-?\d", text, re.M)
    if len(comps) < 4:
        return _fail("general_predict", path,
                     f"scores_block_too_thin({len(comps)} B* keys)", text)
    return _ok("general_predict", path, text)


def qc_general_predict(path: str | Path) -> QCResult:
    p = str(path)
    if not os.path.exists(p):
        return _fail("general_predict", p, "missing", empty=True)
    return qc_text_general_predict(_read(p), p)


def qc_text_sector_predict(text: str, path: str = "") -> QCResult:
    if not (text or "").strip():
        return _fail("sector_predict", path, "empty", empty=True)
    if looks_like_timeout(text):
        return _fail("sector_predict", path, "timeout_stub", text, timeout=True)
    if len(text) < MIN_SECTOR_CHARS:
        return _fail("sector_predict", path, f"too_small({len(text)})", text,
                     empty=True)
    missing = _missing(text, [
        "MEMORY_CONFIRM", "SECTOR_SCORES_BEGIN", "SECTOR_SCORES_END",
        "HIT_GRID_BEGIN",
    ])
    if missing:
        return _fail("sector_predict", path,
                     "missing:" + ",".join(missing), text, missing=missing)
    comps = re.findall(r"^S[0-4]_[A-Z0-9_]+:\s*-?\d", text, re.M)
    if len(comps) < 3:
        return _fail("sector_predict", path,
                     f"scores_block_too_thin({len(comps)} S* keys)", text)
    return _ok("sector_predict", path, text)


def qc_sector_predict(path: str | Path) -> QCResult:
    p = str(path)
    if not os.path.exists(p):
        return _fail("sector_predict", p, "missing", empty=True)
    return qc_text_sector_predict(_read(p), p)


def qc_news_judge(path: str | Path) -> QCResult:
    p = str(path)
    if not os.path.exists(p):
        return _fail("news_judge", p, "missing", empty=True)
    text = _read(p)
    if not text.strip():
        return _fail("news_judge", p, "empty", empty=True)
    if looks_like_timeout(text):
        return _fail("news_judge", p, "timeout_stub", text, timeout=True)
    if len(text) < MIN_JUDGE_CHARS:
        return _fail("news_judge", p, f"too_small({len(text)})", text, empty=True)
    missing = _missing(text, ["NEWS_PARSE_BEGIN"])
    if missing:
        return _fail("news_judge", p, "missing:" + ",".join(missing),
                     text, missing=missing)
    return _ok("news_judge", p, text)


def qc_news_parse(path: str | Path) -> QCResult:
    p = str(path)
    if not os.path.exists(p):
        return _fail("news_parse", p, "missing", empty=True)
    data = _read_json(p)
    if not isinstance(data, dict):
        return _fail("news_parse", p, "unparseable_json", empty=True)
    raw = int(data.get("raw_count") or 0)
    if raw <= 0 and not (data.get("usable_top") or data.get("all_items")):
        return _fail("news_parse", p, "empty_parse", empty=True)
    return _ok("news_parse", p, json.dumps(data)[:50])


def qc_map_heat_research(path: str | Path) -> QCResult:
    p = str(path)
    if not os.path.exists(p):
        return _fail("map_heat_research", p, "missing", empty=True)
    text = _read(p)
    if looks_like_timeout(text):
        return _fail("map_heat_research", p, "timeout_stub", text, timeout=True)
    missing = _missing(text, ["CAPTAIN_CARDS_OK", "OPPORTUNITY_OK"])
    if missing:
        return _fail("map_heat_research", p, "missing:" + ",".join(missing),
                     text, missing=missing)
    js = p.replace("_research.md", "_research.json")
    data = _read_json(js)
    n = len((data or {}).get("cards") or []) if isinstance(data, dict) else 0
    if n < 20:
        return _fail("map_heat_research", p, f"too_few_cards({n})", text)
    bad_evidence = 0
    invented_shape = 0
    missing_x_record = 0
    n_refreshed = int((data or {}).get("n_refreshed") or 0)
    for card_i, card in enumerate((data or {}).get("cards") or []):
        if str(card.get("subsector_dir") or "") not in ("up", "down", "flat"):
            invented_shape += 1
        for cap in card.get("captains") or []:
            sent = str(cap.get("sent") or "none")
            if sent not in ("pos", "neg", "mixed", "none"):
                invented_shape += 1
            ev = cap.get("evidence") or []
            if sent != "none" and not any(
                    isinstance(e, dict)
                    and str(e.get("url") or "").startswith(("http://", "https://"))
                    and e.get("published_at") and e.get("fact")
                    for e in ev):
                bad_evidence += 1
            if card_i < n_refreshed and not isinstance(
                    cap.get("x_sentiment"), dict):
                missing_x_record += 1
    if invented_shape:
        return _fail("map_heat_research", p,
                     f"invalid_card_shape({invented_shape})", text)
    if bad_evidence:
        return _fail("map_heat_research", p,
                     f"sentiment_without_evidence({bad_evidence})", text)
    if missing_x_record:
        return _fail("map_heat_research", p,
                     f"morning_missing_x_record({missing_x_record})", text)
    if data.get("phase") != "morning_refresh":
        return _fail("map_heat_research", p,
                     f"not_morning_refresh({data.get('phase')})", text)
    return _ok("map_heat_research", p, text)


def qc_news_actions(path: str | Path) -> QCResult:
    p = str(path)
    if not os.path.exists(p):
        return _fail("news_actions", p, "missing", empty=True)
    data = _read_json(p)
    if not isinstance(data, dict):
        return _fail("news_actions", p, "unparseable_json", empty=True)
    if "ticker_actions" not in data and "edge_actions" not in data:
        return _fail("news_actions", p, "missing ticker_actions/edge_actions")
    return _ok("news_actions", p, "ok")


def qc_finviz_digest(path: str | Path) -> QCResult:
    p = str(path)
    if not os.path.exists(p):
        return _fail("finviz_digest", p, "missing", empty=True)
    # Prefer JSON; fall back to markdown existence+size.
    if p.endswith(".json"):
        data = _read_json(p)
        if not isinstance(data, dict):
            return _fail("finviz_digest", p, "unparseable_json", empty=True)
        if not (data.get("index_digests") or data.get("ticker_digest_count")
                or data.get("date")):
            return _fail("finviz_digest", p, "empty_digest", empty=True)
        return _ok("finviz_digest", p, "ok")
    text = _read(p)
    if len(text) < 200:
        return _fail("finviz_digest", p, f"too_small({len(text)})", text,
                     empty=True)
    return _ok("finviz_digest", p, text)


def _events_payload(date_str: str) -> tuple[str, dict | None]:
    path = os.path.join("01_daily", "events", f"{date_str}_events.json")
    return path, _read_json(path)


def qc_events_date(date_str: str) -> QCResult:
    path, data = _events_payload(date_str)
    if not os.path.exists(path):
        return _fail("events", path, "missing", empty=True)
    if not isinstance(data, dict):
        return _fail("events", path, "unparseable_json", empty=True)
    text = _read(path)
    md_path = os.path.join("01_daily", "events", f"{date_str}_events.md")
    md = _read(md_path)
    events = data.get("events") or []
    carried_from = data.get("carried_from")
    all_carried = bool(events) and all(
        (e.get("status") == "carried") for e in events if isinstance(e, dict)
    )
    md_carried = bool(CARRIED_RE.search(md) or "CARRIED FORWARD" in md)
    if carried_from or all_carried or md_carried:
        return _fail("events", path,
                     f"carried_from={carried_from or 'md/status'}",
                     text, carried=True)
    if looks_like_timeout(text) or looks_like_timeout(md):
        return _fail("events", path, "timeout_stub", text, timeout=True)
    if not events:
        return _fail("events", path, "zero_events", text, empty=True)
    if data.get("error") == "parse_failed":
        return _fail("events", path, "parse_failed", text, empty=True)
    if len(text) < MIN_EVENTS_CHARS:
        return _fail("events", path, f"too_small({len(text)})", text, empty=True)
    return _ok("events", path, text)


def qc_events_path(path: str | Path) -> QCResult:
    p = str(path)
    m = re.search(r"(\d{4}-\d{2}-\d{2})_events", os.path.basename(p))
    if m:
        return qc_events_date(m.group(1))
    data = _read_json(p)
    if not isinstance(data, dict) or not (data.get("events")):
        return _fail("events", p, "empty_or_unparseable", empty=True)
    if data.get("carried_from"):
        return _fail("events", p, "carried_from", carried=True)
    return _ok("events", p, "ok")


# ---------------------------------------------------------------------------
# Reject / throw out
# ---------------------------------------------------------------------------

def reject(*paths: str | Path) -> None:
    """Delete a bad artifact so skip-if-good cannot treat it as success."""
    for p in paths:
        if not p:
            continue
        path = Path(p)
        if path.exists():
            try:
                path.unlink()
                print(f"[output_qc] threw out {path}")
            except OSError as e:
                print(f"[output_qc] could not delete {path}: {e}")


def reject_events(date_str: str) -> None:
    d = os.path.join("01_daily", "events")
    reject(
        os.path.join(d, f"{date_str}_events.json"),
        os.path.join(d, f"{date_str}_events.md"),
    )
    # latest.* may still point at the carried copy — only clear it if it
    # is itself a carry / empty, so we don't clobber a good older pointer
    # from a different date during a botched same-day retry.
    latest = os.path.join(d, "latest.json")
    data = _read_json(latest)
    if isinstance(data, dict) and (
            data.get("carried_from") or data.get("scan_date") == date_str):
        reject(latest, os.path.join(d, "latest.md"))


# ---------------------------------------------------------------------------
# Day-wide pre-open report
# ---------------------------------------------------------------------------

def _slug(sector: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", sector.lower()).strip("_")


def preopen_report(date_str: str) -> dict:
    """Inspect every pre-open artifact for `date_str`. Does not write."""
    items: list[QCResult] = []
    items.append(qc_general_predict(
        os.path.join(config.DAILY_GENERAL, f"{date_str}_predict.md")))
    items.append(qc_events_date(date_str))
    items.append(qc_news_judge(os.path.join("01_daily", "news",
                                            f"{date_str}_judge.md")))
    items.append(qc_news_parse(os.path.join("01_daily", "news",
                                            f"{date_str}_parsed.json")))
    items.append(qc_news_actions(os.path.join("01_daily", "news",
                                              f"{date_str}_actions.json")))
    digest_json = os.path.join("01_daily", "news",
                               f"{date_str}_finviz_digest.json")
    digest_md = os.path.join("01_daily", "news",
                             f"{date_str}_finviz_digest.md")
    items.append(qc_finviz_digest(
        digest_json if os.path.exists(digest_json) else digest_md))
    items.append(qc_map_heat_research(
        os.path.join("01_daily", "map_heat", f"{date_str}_research.md")))

    sector_rows = []
    for sector in FINVIZ_SECTORS:
        p = os.path.join(config.DAILY_SECTORS, date_str,
                         f"{_slug(sector)}_predict.md")
        r = qc_sector_predict(p)
        items.append(r)
        sector_rows.append({"sector": sector, "ok": r.ok,
                            "reason": r.reason, "size": r.size})
    n_ok = sum(1 for r in sector_rows if r["ok"])

    report = {
        "date": date_str,
        "generated_at": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "items": [asdict(r) for r in items],
        "sectors": sector_rows,
        "sector_n_ok": n_ok,
        "sector_n_total": len(FINVIZ_SECTORS),
        "all_ok": all(r.ok for r in items
                      if r.kind not in ("sector_predict", "map_heat_research"))
                  and n_ok >= 8,
    }
    return report


def write_preopen_report(date_str: str) -> str:
    report = preopen_report(date_str)
    os.makedirs("01_daily", exist_ok=True)
    path = os.path.join("01_daily", f"{date_str}_preopen_qc.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    sec_dir = os.path.join(config.DAILY_SECTORS, date_str)
    if os.path.isdir(sec_dir):
        sidecar = os.path.join(sec_dir, "_qc.json")
        with open(sidecar, "w", encoding="utf-8") as fh:
            json.dump({
                "date": date_str,
                "n_ok": report["sector_n_ok"],
                "n_total": report["sector_n_total"],
                "sectors": report["sectors"],
            }, fh, indent=2)
    return path


def render(report: dict) -> str:
    lines = [f"[preopen-qc] {report['date']}  "
             f"sectors {report['sector_n_ok']}/{report['sector_n_total']}  "
             f"all_ok={report['all_ok']}"]
    for r in report["items"]:
        flag = "OK  " if r["ok"] else "FAIL"
        reason = f"  {r['reason']}" if r.get("reason") else ""
        lines.append(f"  [{flag}] {r['kind']:<16} {r['path']}{reason}")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--preopen", action="store_true",
                    help="Scan every pre-open artifact for --date")
    ap.add_argument("--kind", default="",
                    help="general|sector|events|judge|parse|actions|digest|heat_research")
    ap.add_argument("--path", default="")
    ap.add_argument("--write", action="store_true",
                    help="Write 01_daily/<date>_preopen_qc.json")
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()

    if args.path and args.kind:
        kind = args.kind.lower()
        dispatch = {
            "general": qc_general_predict,
            "sector": qc_sector_predict,
            "events": qc_events_path,
            "judge": qc_news_judge,
            "parse": qc_news_parse,
            "actions": qc_news_actions,
            "digest": qc_finviz_digest,
            "heat_research": qc_map_heat_research,
        }
        fn = dispatch.get(kind)
        if not fn:
            raise SystemExit(f"unknown kind {args.kind!r}")
        r = fn(args.path)
        print(r.explain())
        raise SystemExit(0 if r.ok else 1)

    report = preopen_report(date_str)
    print(render(report))
    if args.write or args.preopen:
        path = write_preopen_report(date_str)
        print(f"[output_qc] wrote {path}")
    raise SystemExit(0 if report["all_ok"] else 1)


if __name__ == "__main__":
    main()
