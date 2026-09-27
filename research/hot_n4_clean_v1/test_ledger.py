"""Ledger, variant count, and pin checks. Does not import a scorer."""
from __future__ import annotations

import hashlib
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v1.protocol import (  # noqa: E402
    ACTIONS_SHA256,
    ALL_SOURCES,
    ALWAYS_ABSENT,
    AUG_SHA256,
    CAP_V3_LUCK_N,
    ENGINE_SHA256,
    FEES_SHA256,
    DELISTED,
    FIRST_HALT_SESSION,
    LIQ_CAP_FRAC,
    LIQ_DAYS,
    FORWARD,
    FULLSCAN_CSV_SHA256,
    GAP_DAY,
    LUCK_N,
    N_VARIANTS,
    OHLC_SHA256,
    OUTCOME_KEYS,
    PRICE_SOURCES,
    PREREG,
    PROVENANCE_SHA256,
    RETRO_ACTIONS_SHA256,
    SESSIONS,
    SPLITS,
    SPLITS_MD,
    SPLITS_MD_SHA256,
    STARTS,
    SLIP_LINES,
    SLIP_PRIMARY,
    S_SOURCE,
    TUNE,
    UNEXPLAINED,
    WILSON_Z,
    VARIANTS,
    WEATHER_DAYS,
    commit_day,
    file_sha256,
    fingerprint_sha256,
    sources_on,
    verify_ledger,
)


def _header(text: str, key: str) -> str:
    for line in text.splitlines():
        if line.startswith(f"- {key}:"):
            return line.split(":", 1)[1].strip().strip("`")
    raise SystemExit(f"missing {key}")


def _pins(text: str) -> None:
    for digest in (
        OHLC_SHA256, ACTIONS_SHA256, RETRO_ACTIONS_SHA256, FEES_SHA256,
        AUG_SHA256, SPLITS_MD_SHA256, PROVENANCE_SHA256, FULLSCAN_CSV_SHA256,
    ):
        if digest not in text:
            raise SystemExit(f"pin missing from prereg {digest[:12]}")
    for path, digest in ENGINE_SHA256.items():
        if digest not in text:
            raise SystemExit(f"engine pin missing {path}")
        got = file_sha256(ROOT / path)
        if got != digest:
            raise SystemExit(f"engine bytes changed {path}")


def _rules(text: str) -> None:
    for phrase in (
        "commit_day", "LEDGER.jsonl", "13:30 UTC", "Earnings Date",
        "not published", "signal_alarm", "purely_worse", "BOX_COLS",
        "union_hot_n4_h1__w0", "union_hot_n4_holdup__w0", "keep_held",
        "22011", "2026-08-28", "nonews",
        "0.5%", "Wilson", "What v4", "1.96", "AAC-U",
    ):
        if phrase not in text:
            raise SystemExit(f"rule missing: {phrase}")
    if file_sha256(SPLITS_MD) != SPLITS_MD_SHA256:
        raise SystemExit("SPLITS.md hash")
    if fingerprint_sha256(text) != _header(text, "fingerprint_sha256"):
        raise SystemExit("fingerprint")


def _variants() -> None:
    if N_VARIANTS != 2 or len(VARIANTS) != 2:
        raise SystemExit("variant count")
    ids = [row["id"] for row in VARIANTS]
    if ids != ["union_hot_n4_h1__w0", "union_hot_n4_holdup__w0"]:
        raise SystemExit("ids")
    if VARIANTS[0]["s_boost"] != "none" or VARIANTS[1]["s_boost"] != "holdup":
        raise SystemExit("boost")
    for row in VARIANTS:
        if row["weather"] or row["forbid"] or row["exit_when"]:
            raise SystemExit("weather or alarm leaked in")
        if row["top_n"] != 4 or row["hold"] != 1 or row["sell"] != "list":
            raise SystemExit("shape")
        if row["rank"] != "hot_score" or row["side"] != "long":
            raise SystemExit("rank")
    if CAP_V3_LUCK_N != 22009 or LUCK_N != 22011:
        raise SystemExit("luck")
    if LUCK_N != CAP_V3_LUCK_N + N_VARIANTS:
        raise SystemExit("luck add")


def _calendar(text: str) -> None:
    if len(SESSIONS) != 31 or len(TUNE) != 21 or len(FORWARD) != 10:
        raise SystemExit("sessions")
    if TUNE[-1] != "2026-09-11" or FORWARD[0] != "2026-09-14":
        raise SystemExit("split")
    if "2026-09-07" in SESSIONS:
        raise SystemExit("labor day")
    if STARTS != ("2026-08-17", "2026-08-24", "2026-08-31"):
        raise SystemExit("starts")
    if len(SPLITS) != 9:
        raise SystemExit("splits")
    tickers = [row[1] for row in SPLITS]
    if tickers != ["BYND", "AVB", "NRDY", "SFBS", "BRCC", "REAX", "RUSHA", "APH", "IGR"]:
        raise SystemExit("split names")
    if [row[0] for row in UNEXPLAINED] != ["XHG", "EYPT", "SMJF", "ADBT"]:
        raise SystemExit("unexplained")
    if FIRST_HALT_SESSION != "2026-08-17":
        raise SystemExit("halt")
    if SLIP_PRIMARY != 0.005 or SLIP_LINES != (0.0, 0.005, 0.01):
        raise SystemExit("slip")
    if LIQ_CAP_FRAC != 0.01 or LIQ_DAYS != 20 or WILSON_Z != 1.96:
        raise SystemExit("cap or wilson")
    if len(DELISTED) != 31 or DELISTED[10] != ("AAC-U", "2026-09-08", None):
        raise SystemExit("delist")
    for ticker, _panel, _bar in DELISTED:
        if ticker not in text:
            raise SystemExit(f"delist missing {ticker}")


def _sources() -> None:
    if set(ALWAYS_ABSENT) != {
        "earn_react", "flatten", "mover_buy", "overnight", "overnight_mega",
    }:
        raise SystemExit("absent")
    if len(S_SOURCE) != 31 or [row[0] for row in S_SOURCE] != list(SESSIONS):
        raise SystemExit("S rows")
    if WEATHER_DAYS != ("2026-08-25", "2026-08-27", "2026-09-08", "2026-09-09"):
        raise SystemExit("weather days")
    for session, kind, status, stamp in S_SOURCE:
        if status not in ("PROVEN", "PROVEN_BUT_CHANGED"):
            raise SystemExit("status")
        if not stamp.startswith(session) or stamp[11:16] >= "13:30":
            raise SystemExit(f"open clock {session}")
        if kind not in ("predict", "weather"):
            raise SystemExit("kind")
    for session in SESSIONS:
        present = sources_on(session)
        if set(present) != set(ALL_SOURCES):
            raise SystemExit("source keys")
        if session == GAP_DAY:
            if any(value == "present" for value in present.values()):
                raise SystemExit("gap day")
            continue
        for name in PRICE_SOURCES:
            if present[name] != "present":
                raise SystemExit(f"missing {name} {session}")
        for name in ALWAYS_ABSENT:
            if present[name] != "absent":
                raise SystemExit(f"present {name} {session}")


def _ledger() -> None:
    verify_ledger()
    with tempfile.TemporaryDirectory() as tmp:
        days = Path(tmp)
        (days / "LEDGER.jsonl").write_bytes(b"")
        verify_ledger(days)
        payload = {"session": SESSIONS[0], "sources": sources_on(SESSIONS[0])}
        digest = commit_day(SESSIONS[0], payload, days_dir=days)
        verify_ledger(days)
        again = commit_day(SESSIONS[0], payload, days_dir=days)
        if again != digest:
            raise SystemExit("rerun digest")
        text = (days / "LEDGER.jsonl").read_text(encoding="utf-8")
        if text.count("\n") != 1:
            raise SystemExit("ledger grew on a matching rerun")
        changed = dict(payload, sources={"ohlc_hot": "absent"})
        before = (days / f"{SESSIONS[0]}.json").read_bytes()
        try:
            commit_day(SESSIONS[0], changed, days_dir=days)
        except RuntimeError:
            pass
        else:
            raise SystemExit("mismatch was accepted")
        if (days / f"{SESSIONS[0]}.json").read_bytes() != before:
            raise SystemExit("mismatch overwrote")
        if (days / "LEDGER.jsonl").read_text(encoding="utf-8") != text:
            raise SystemExit("mismatch appended")
        try:
            commit_day(SESSIONS[1], {"session": SESSIONS[1], "pnl": 1}, days_dir=days)
        except RuntimeError:
            pass
        else:
            raise SystemExit("outcome key accepted")
        if (days / f"{SESSIONS[1]}.json").exists():
            raise SystemExit("outcome key wrote a day")
        try:
            commit_day(SESSIONS[2], {"session": SESSIONS[2]}, days_dir=days)
        except RuntimeError:
            pass
        else:
            raise SystemExit("skip was accepted")
        if "pnl" not in OUTCOME_KEYS or "win_rate" not in OUTCOME_KEYS:
            raise SystemExit("outcome keys")
        # a clean second day still appends once
        commit_day(SESSIONS[1], {"session": SESSIONS[1], "holdup_file": True}, days_dir=days)
        verify_ledger(days)


def main() -> None:
    text = PREREG.read_text(encoding="utf-8")
    _pins(text)
    _rules(text)
    _variants()
    _calendar(text)
    _sources()
    _ledger()
    # local price and audit bytes, when this checkout has them
    for path, digest in (
        ("data/prices/ohlc.parquet", OHLC_SHA256),
        ("data/prices/actions.parquet", ACTIONS_SHA256),
        ("data/factor_mine/retro_prices/actions.parquet", RETRO_ACTIONS_SHA256),
        ("00_grounding/futubull_fees.json", FEES_SHA256),
        ("research/audit/INPUT_PROVENANCE_336.md", PROVENANCE_SHA256),
        ("research/audit/FULLSCAN_FILE_PROOF.csv", FULLSCAN_CSV_SHA256),
    ):
        got = hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
        if got != digest:
            raise SystemExit(f"blob changed {path}")
    print("ledger ok")


if __name__ == "__main__":
    main()
