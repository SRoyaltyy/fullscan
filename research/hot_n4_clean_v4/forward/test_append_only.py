"""Append-only holdup log. Fails if a sealed line changes, and checks the seed."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.append_check import check_against  # noqa: E402
from research.hot_n4_clean_v4.forward.book import H1, reset_book, use_book  # noqa: E402
from research.hot_n4_clean_v4.forward.ledger import (  # noqa: E402
    RECIPE,
    append_records,
    load,
    log_path,
    session_dates,
)
from research.hot_n4_clean_v4.forward.render import LOG_JSON  # noqa: E402
from research.hot_n4_clean_v4.forward.seed import _payloads, build_bodies  # noqa: E402
from research.hot_n4_clean_v4.forward.ledger import canonical_bytes  # noqa: E402
from research.hot_n4_clean_v4.protocol import DAYS, SESSIONS  # noqa: E402
from research.hot_n4_clean_v4.run_study import load_bars, load_fees  # noqa: E402

PAGE = ROOT / "dashboard" / "holdup" / "index.html"


def _body(day: str, kind: str = "session") -> dict:
    if kind == "session":
        return {
            "buys": [],
            "cash_primary": 10000.0,
            "date": day,
            "equity_primary": 10000.0,
            "excluded_unexplained_legs": [],
            "holdings": [],
            "holdup_on": False,
            "kind": "session",
            "morning_s": None,
            "recipe": RECIPE,
            "sells": [],
            "unfilled": [],
        }
    return {
        "cost_model": "futubull+0.5%/side",
        "date": day,
        "entry_date": "2026-08-13",
        "entry_px": 1.0,
        "fill": 1.1,
        "kind": "close",
        "pnl_primary": 0.1,
        "reason": "hold-expired",
        "recipe": RECIPE,
        "shares": 1,
        "ticker": "ZZ",
    }


def _temp() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        first = append_records([_body("2026-08-13")], folder)
        before = log_path(folder).read_bytes()
        try:
            append_records([_body("2026-08-13")], folder)
        except RuntimeError:
            pass
        else:
            raise SystemExit("duplicate session was accepted")
        if log_path(folder).read_bytes() != before:
            raise SystemExit("duplicate session wrote")
        day = _body("2026-08-14")
        day["sells"] = [{
            "fill": 1.1,
            "reason": "hold-expired",
            "shares": 1,
            "ticker": "ZZ",
        }]
        append_records([day, _body("2026-08-14", "close")], folder)
        if not log_path(folder).read_bytes().startswith(before):
            raise SystemExit("later append rewrote the first line")
        records = load(folder)
        if session_dates(records) != ["2026-08-13", "2026-08-14"]:
            raise SystemExit("temp sessions")
        if records[0]["sha256"] != first[0]["sha256"]:
            raise SystemExit("first hash changed")
        raw = bytearray(log_path(folder).read_bytes())
        raw[0] = raw[0] ^ 1
        log_path(folder).write_bytes(raw)
        try:
            load(folder)
        except RuntimeError:
            pass
        else:
            raise SystemExit("tampered line passed")


def _seed_shape(records: list[dict]) -> None:
    dates = session_dates(records)
    if dates[:len(SESSIONS)] != list(SESSIONS):
        raise SystemExit("seed prefix")
    if len(dates) < len(SESSIONS):
        raise SystemExit("seed short")
    closes = [row for row in records if row["kind"] == "close"]
    if not closes:
        raise SystemExit("no closes")
    for row in records:
        if row["kind"] != "session":
            continue
        card = json.loads((DAYS / f"{row['date']}.json").read_text(encoding="utf-8"))
        if row["excluded_unexplained_legs"] != card["excluded_unexplained_legs"]:
            raise SystemExit(f"excluded legs {row['date']}")
        for buy in row["buys"]:
            if buy.get("fill_basis") != "open":
                raise SystemExit("fill basis")
            if "pnl" in buy or "pnl_primary" in buy:
                raise SystemExit("pnl on buy")
    reasons = {row["reason"] for row in closes}
    if not reasons <= {"hold-expired", "holdup extension"}:
        raise SystemExit(f"reasons {reasons}")


def _replay(records: list[dict]) -> None:
    print("replaying sealed sessions against returns/", flush=True)
    bars = load_bars()
    fees = load_fees()
    bodies = build_bodies(_payloads(), bars, fees)
    seeded = []
    for record in records:
        if record["kind"] == "session" and record["date"] not in set(SESSIONS):
            break
        if record["kind"] == "session" and record["date"] in set(SESSIONS):
            seeded.append(record)
            continue
        if record["kind"] == "close" and record["date"] in set(SESSIONS):
            seeded.append(record)
    if len(seeded) != len(bodies):
        raise SystemExit(f"seed length {len(seeded)} {len(bodies)}")
    for got, exp in zip(seeded, bodies):
        body = {key: value for key, value in got.items() if key != "sha256"}
        if canonical_bytes(body) != canonical_bytes(exp):
            raise SystemExit(f"replay bytes differ {exp['kind']} {exp.get('date')} {exp.get('ticker')}")
    print("seed matches returns/ and the sealed log")


def _page(records: list[dict]) -> None:
    text = PAGE.read_text(encoding="utf-8")
    for phrase in ("22,017", "append-only", "union_hot_n4_holdup__w0", "log.json", "0.5%"):
        if phrase not in text:
            raise SystemExit(f"page missing {phrase}")
    rendered = json.loads(LOG_JSON.read_text(encoding="utf-8"))
    if rendered != records:
        raise SystemExit("log.json is not the sealed log")


def _h1(records: list[dict]) -> None:
    dates = session_dates(records)
    if dates != list(SESSIONS):
        raise SystemExit("h1 seed sessions")
    if any(row["kind"] == "plan" and row["date"] == "2026-09-28" for row in records):
        raise SystemExit("h1 wrote a 2026-09-28 plan after the open")
    closes = [row for row in records if row["kind"] == "close"]
    buys = [row for row in records if row["kind"] == "session" for row in row["buys"]]
    if len(closes) != 80 or len(buys) != 84:
        raise SystemExit(f"h1 trades closes {len(closes)} buys {len(buys)}")
    reasons = {row["reason"] for row in closes}
    if reasons != {"hold-expired"}:
        raise SystemExit(f"h1 reasons {reasons}")
    page = ROOT / "dashboard" / "h1" / "index.html"
    text = page.read_text(encoding="utf-8")
    for phrase in ("union_hot_n4_h1__w0", "append-only", "2026-09-28", "no sealed", "0.5%", "log.json"):
        if phrase not in text:
            raise SystemExit(f"h1 page missing {phrase}")
    rendered = json.loads((H1.page / "log.json").read_text(encoding="utf-8"))
    if rendered != records:
        raise SystemExit("h1 log.json is not the sealed log")
    status = json.loads((H1.page / "status.json").read_text(encoding="utf-8"))
    if "2026-09-28" not in (status.get("note") or ""):
        raise SystemExit("h1 status does not record the missing 2026-09-28 plan")


def main() -> None:
    _temp()
    records = load()
    _seed_shape(records)
    _page(records)
    check_against("origin/main")
    _replay(records)
    print("holdup append-only ok", len(records), "records")
    token = use_book(H1)
    try:
        h1 = load()
        _h1(h1)
        _replay(h1)
    finally:
        reset_book(token)
    print("h1 append-only ok", "seed matches returns/")


if __name__ == "__main__":
    main()
