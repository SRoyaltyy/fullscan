"""Pin labelled inputs for factor_mine_score_search_v3. No score."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
STUDY = Path(__file__).resolve().parent
OUT = STUDY / "INPUTS.json"
FULLSCAN_BLOB = "832eaa4fdc19d368d87a4dad5c3edd3c200b0a54"
FULLSCAN_SHA256 = "1bfafd9d4f9a512b9bc3ac33d6d087349028f57ec4d0fedffe76ee0998e48f75"
EXCEL_PATH = ROOT / "research" / "lever_search" / "excel_preopen_proof.csv"
LABEL = "assumed pre-open, not server-proven"
PREDICT_RE = re.compile(
    r"Prediction:\s*(UP|DOWN|FLAT).*?total score\s*(-?[\d.]+)",
    re.I | re.S,
)
SIGNAL_RE = re.compile(r"signal_date=(\d{4}-\d{2}-\d{2})")

SESSIONS = (
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19",
    "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26",
    "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02",
    "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10",
    "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17",
    "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24",
    "2026-09-25",
)


def _git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(args, cwd=ROOT, check=False, capture_output=True)


def _blob(sha: str) -> bytes:
    proc = _git("git", "cat-file", "blob", sha)
    if proc.returncode != 0:
        raise SystemExit(f"blob {sha}")
    return proc.stdout


def earliest_commit(path: str) -> str | None:
    proc = _git("git", "log", "--diff-filter=A", "--reverse", "--pretty=%H", "--", path)
    if proc.returncode != 0:
        return None
    line = proc.stdout.decode().splitlines()
    return line[0].strip() if line else None


def blob_at(commit: str, path: str) -> str | None:
    proc = _git("git", "rev-parse", f"{commit}:{path}")
    if proc.returncode != 0:
        return None
    return proc.stdout.decode().strip()


def _num(value) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "").replace("%", "")
    if not text or text.lower() in {"nan", "none", "—", "-"}:
        return None
    try:
        out = float(text)
    except ValueError:
        return None
    if out != out:
        return None
    return out


def _polarity(value) -> str:
    number = _num(value)
    if number is None:
        return "missing"
    if number > 0.5:
        return "good"
    if number < -0.5:
        return "bad"
    return "neutral"


def _knowable(earnings, session: str) -> bool:
    from src.finviz_events import parse_finviz_datetime

    ed, hm = parse_finviz_datetime(earnings)
    if not ed:
        return True
    if ed < session:
        return True
    if ed == session and (hm is None or int(hm) <= 930):
        return True
    return False


def _predict_score(blob: bytes) -> float | None:
    text = blob.decode("utf-8", errors="replace")
    matched = PREDICT_RE.search(text)
    if not matched:
        return None
    return float(matched.group(2))


def _weather_score(blob: bytes) -> float | None:
    try:
        payload = json.loads(blob)
    except json.JSONDecodeError:
        return None
    value = (payload.get("signals") or {}).get("general_score")
    return _num(value)


def _book_names(blob: bytes) -> list[str]:
    payload = json.loads(blob)
    book = (payload.get("books") or {}).get("1d") or {}
    names = []
    seen = set()
    for row in book.get("buy") or []:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        names.append(ticker)
    return names


def _earn_map(blob: bytes, session: str) -> dict[str, str]:
    text = blob.decode("utf-8", errors="replace")
    out = {}
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None or "Ticker" not in reader.fieldnames:
        return out
    if "EPS Surprise" not in reader.fieldnames:
        return out
    for row in reader:
        ticker = str(row.get("Ticker") or "").strip().upper()
        if not ticker:
            continue
        if not _knowable(row.get("Earnings Date"), session):
            continue
        pol = _polarity(row.get("EPS Surprise"))
        if pol in {"good", "bad"}:
            out[ticker] = pol
    return out


def _load_audit() -> dict[tuple[str, str], dict]:
    raw = _blob(FULLSCAN_BLOB)
    if hashlib.sha256(raw).hexdigest() != FULLSCAN_SHA256:
        raise SystemExit("fullscan sha")
    out = {}
    for row in csv.DictReader(io.StringIO(raw.decode("utf-8"))):
        out[(row["date"], row["input"])] = row
    return out


def _panel_by_session() -> dict[str, dict]:
    """Earliest commit of panel.json whose rows are labelled with each session."""
    proc = _git("git", "log", "--reverse", "--pretty=%H", "origin/main", "--", "data/factor_mine/panel.json")
    if proc.returncode != 0:
        raise SystemExit("panel log")
    commits = [line.strip() for line in proc.stdout.decode().splitlines() if line.strip()]
    found: dict[str, dict] = {}
    need = set(SESSIONS)
    for commit in commits:
        if not need:
            break
        sha = blob_at(commit, "data/factor_mine/panel.json")
        if not sha:
            continue
        payload = json.loads(_blob(sha))
        by_date: dict[str, dict[str, list[int]]] = {}
        for row in payload.get("rows") or []:
            if not isinstance(row, dict):
                continue
            date = str(row.get("date") or "")[:10]
            ticker = str(row.get("ticker") or "").strip().upper()
            if date not in need or not ticker:
                continue
            by_date.setdefault(date, {})[ticker] = [
                int(row.get("cond_good") or 0),
                int(row.get("cond_bad") or 0),
            ]
        for date, cam in by_date.items():
            if date in found:
                continue
            found[date] = {"blob": sha, "commit": commit, "cam": cam}
            need.discard(date)
        print(f"panel {commit[:12]} still {len(need)}", flush=True)
    return found


def _excel() -> dict[str, dict]:
    raw = EXCEL_PATH.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    out = {"sha256": digest, "sessions": {}}
    for row in csv.DictReader(io.StringIO(raw.decode("utf-8"))):
        session = row["session_date"]
        if session not in set(SESSIONS):
            continue
        note = row.get("diff_note") or ""
        matched = SIGNAL_RE.search(note)
        proven = str(row.get("proven") or "").strip().lower() == "true"
        status = row.get("status") or ""
        n_pre = int(row.get("n_signal_rows_preopen") or 0)
        out["sessions"][session] = {
            "n_preopen": n_pre,
            "proven": proven and status in {"PROVEN", "PROVEN_BUT_CHANGED"} and n_pre > 0,
            "signal_date": matched.group(1) if matched else None,
            "status": status,
        }
    return out


def _s_primary(session: str) -> tuple[float | None, str | None]:
    predict = f"01_daily/general/{session}_predict.md"
    commit = earliest_commit(predict)
    if commit:
        sha = blob_at(commit, predict)
        if sha:
            score = _predict_score(_blob(sha))
            if score is not None:
                return score, sha
    weather = f"01_daily/weather/{session}_weather.json"
    commit = earliest_commit(weather)
    if commit:
        sha = blob_at(commit, weather)
        if sha:
            return _weather_score(_blob(sha)), sha
    return None, None


def _s_strict(audit: dict, session: str) -> float | None:
    row = audit.get((session, "S")) or {}
    if str(row.get("proven") or "").lower() != "yes" or not row.get("blob_sha"):
        return None
    blob = _blob(row["blob_sha"])
    path = row.get("path") or ""
    if path.endswith("_predict.md"):
        return _predict_score(blob)
    if path.endswith("_weather.json"):
        return _weather_score(blob)
    return None


def _named_source(audit: dict, session: str, kind: str, path: str, parser, *, dated: bool) -> dict:
    commit = earliest_commit(path)
    primary = {"on": False}

    def _parse(blob: bytes):
        return parser(blob, session) if dated else parser(blob)

    if commit:
        sha = blob_at(commit, path)
        if sha:
            primary = {"blob": sha, "commit": commit, "on": True, "names": _parse(_blob(sha))}
    row = audit.get((session, kind)) or {}
    strict = {"on": False}
    proven = str(row.get("proven") or "").lower() == "yes" and bool(row.get("blob_sha"))
    if proven:
        sha = row["blob_sha"]
        strict = {"blob": sha, "on": True, "names": _parse(_blob(sha))}
    return {"primary": primary, "strict": strict}


def build() -> dict:
    audit = _load_audit()
    panel = _panel_by_session()
    excel = _excel()
    dates = {}
    for session in SESSIONS:
        book_path = f"data/stock_book/{session}_stock_book.json"
        book = _named_source(audit, session, "stock_book", book_path, _book_names, dated=False)
        export_path = f"data/exports/finviz_{session}.csv"
        earn = _named_source(audit, session, "export", export_path, _earn_map, dated=True)
        s_primary, s_blob = _s_primary(session)
        s_strict = _s_strict(audit, session)
        cam = panel.get(session)
        primary_names = book["primary"].get("names") or []
        strict_names = book["strict"].get("names") or []
        dates[session] = {
            "cam": None if cam is None else {
                "blob": cam["blob"],
                "commit": cam["commit"],
                "rows": cam["cam"],
            },
            "earn_primary": {
                "blob": earn["primary"].get("blob"),
                "on": bool(earn["primary"].get("on")),
                "pol": earn["primary"].get("names") or {},
            },
            "earn_strict": {
                "blob": earn["strict"].get("blob"),
                "on": bool(earn["strict"].get("on")),
                "pol": earn["strict"].get("names") or {},
            },
            "excel": excel["sessions"].get(session),
            "primary_source": "stock_book" if primary_names else "price_only",
            "s_blob": s_blob,
            "s_primary": s_primary,
            "s_strict": s_strict,
            "stock_book_primary": {
                "blob": book["primary"].get("blob"),
                "names": primary_names,
            },
            "stock_book_strict": {
                "blob": book["strict"].get("blob"),
                "names": strict_names,
            },
            "strict_source": "stock_book" if strict_names else "price_only",
        }
        print(
            session,
            dates[session]["primary_source"],
            len(primary_names),
            dates[session]["strict_source"],
            len(strict_names),
            "S", s_primary, s_strict,
            "earn", dates[session]["earn_primary"]["on"], dates[session]["earn_strict"]["on"],
            "cam", 0 if cam is None else len(cam["cam"]),
            flush=True,
        )
    return {
        "dates": dates,
        "excel_sha256": excel["sha256"],
        "fullscan_blob": FULLSCAN_BLOB,
        "fullscan_sha256": FULLSCAN_SHA256,
        "label": LABEL,
        "sessions": list(SESSIONS),
    }


def main() -> None:
    payload = build()
    raw = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    OUT.write_bytes(raw)
    print("sha256", hashlib.sha256(raw).hexdigest(), "bytes", len(raw), flush=True)


if __name__ == "__main__":
    main()
