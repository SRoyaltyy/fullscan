"""Walk a fresh $10,000 book from every start day. Writes the report. Does not pick a recipe."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.concentration_cap_v1.engine import walk as cap_walk  # noqa: E402
from research.concentration_cap_v1.metrics import slice_book as cap_slice  # noqa: E402
from research.concentration_cap_v1.metrics import window_row  # noqa: E402
from research.concentration_screen_v1.metrics import period_stats  # noqa: E402
from research.concentration_screen_v1.metrics import slice_book as screen_slice  # noqa: E402
from research.factor_mine_recipe_search_v4.bars import CleanStore  # noqa: E402
from research.factor_mine_recipe_search_v4.engine import walk as v4_walk  # noqa: E402
from research.factor_mine_recipe_search_v4.protocol import (  # noqa: E402
    CAPITAL,
    FORWARD,
    INPUTS,
    SESSIONS,
    TUNE,
)
from research.start_day_sweep_v1.protocol import (  # noqa: E402
    END_SESSION,
    FIRST_START,
    LAST_START,
    LUCK_N,
    NEW_TRIES,
    P2,
    SCREEN_STARTS,
    START_DAYS,
    assert_recipe_grid,
    ending_return,
    recipes,
    summarize_p2,
    summarize_starts,
)
from research.start_day_sweep_v1.report import render  # noqa: E402
from src.paper_trade import load_fees  # noqa: E402

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "RESULTS.json"
REPORT = HERE / "REPORT.md"
MATCH_TOL = 1e-8


def _days_by_session(payload: dict) -> dict:
    dates = payload["dates"]
    if list(dates) != list(SESSIONS):
        raise SystemExit("pinned board sessions")
    return {
        session: {
            "session": session,
            "s": dates[session]["s"],
            "rows": dates[session]["rows"],
        }
        for session in SESSIONS
    }


def _suffix(days_by: dict, start: str) -> list[dict]:
    return [days_by[session] for session in SESSIONS if session >= start]


def _walk(spec: dict, days: list[dict], fees: dict, price) -> dict:
    recipe = spec["recipe"]
    if spec["engine"] == "cap":
        return cap_walk(days, recipe, fees, price)
    if spec["engine"] == "v4":
        return v4_walk(days, recipe, fees, price, "keep_held")
    raise SystemExit(spec["engine"])


def _num_diff(got, exp) -> float | None:
    if got is None and exp is None:
        return 0.0
    if got is None or exp is None:
        return None
    return abs(float(got) - float(exp))


def _check(checks: list[dict], *, group: str, kind: str, field: str, got, exp, recipe: str, start: str) -> None:
    diff = _num_diff(got, exp)
    checks.append({
        "abs_diff": diff,
        "expected": exp,
        "field": field,
        "got": got,
        "group": group,
        "kind": kind,
        "recipe": recipe,
        "start": start,
    })


def _screen_periods(book: dict, start: str) -> tuple[dict, dict]:
    p1_sessions = [day for day in TUNE if day >= start]
    if not p1_sessions:
        raise SystemExit(f"no P1 for {start}")
    p1_end = next(day["equity"] for day in book["daily"] if day["session"] == p1_sessions[-1])
    p1 = period_stats(screen_slice(book, p1_sessions, CAPITAL))
    p2 = period_stats(screen_slice(book, FORWARD, p1_end))
    return p1, p2


def _compare_screen(checks: list[dict], book: dict, recipe_id: str, start: str, ref: dict, group: str) -> None:
    p1, p2 = _screen_periods(book, start)
    for period, got in (("p1", p1), ("p2", p2)):
        exp = ref[period]
        for field, kind in (
            ("ret", "return"),
            ("ret_15", "return"),
            ("n_trades", "count"),
            ("start_equity", "equity"),
        ):
            _check(
                checks,
                group=group,
                kind=kind,
                field=f"{period}_{field}",
                got=got[field],
                exp=exp[field],
                recipe=recipe_id,
                start=start,
            )


def _compare_cap_window(checks: list[dict], got: dict, exp: dict, recipe_id: str, start: str, group: str) -> None:
    for field, kind in (
        ("compound", "return"),
        ("compound_15", "return"),
        ("n", "count"),
        ("win_rate", "return"),
        ("end_equity", "equity"),
        ("start_equity", "equity"),
    ):
        _check(
            checks,
            group=group,
            kind=kind,
            field=field,
            got=got.get(field),
            exp=exp.get(field),
            recipe=recipe_id,
            start=start,
        )


def _failed(checks: list[dict], recipe_id: str) -> list[dict]:
    bad = []
    for row in checks:
        if row["recipe"] != recipe_id:
            continue
        diff = row["abs_diff"]
        if diff is None or diff > MATCH_TOL:
            bad.append(row)
    return bad


def _max_diff(checks: list[dict], *, group: str | None = None, kind: str | None = None) -> float:
    values = []
    for row in checks:
        if group is not None and row["group"] != group:
            continue
        if kind is not None and row["kind"] != kind:
            continue
        if row["abs_diff"] is None:
            return float("inf")
        values.append(float(row["abs_diff"]))
    if not values:
        raise SystemExit("no sanity checks")
    return max(values)


def _start_row(book: dict) -> dict:
    last = book["daily"][-1]
    if last["session"] != END_SESSION:
        raise SystemExit("book did not reach the end session")
    end_equity = float(last["equity"])
    end_equity_15 = float(last["equity_15"])
    return {
        "end_equity": end_equity,
        "end_equity_15": end_equity_15,
        "ret": ending_return(end_equity),
        "ret_15": ending_return(end_equity_15),
        "session": book["daily"][0]["session"],
    }


def _load_screen() -> dict[str, dict]:
    path = ROOT / "research" / "concentration_screen_v1" / "RESULTS.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {row["id"]: row for row in payload["rows"]}


def _load_cap() -> tuple[dict, dict]:
    folder = ROOT / "research" / "concentration_cap_v3" / "returns"
    tune = json.loads((folder / "TUNE.json").read_text(encoding="utf-8"))
    forward = json.loads((folder / "FORWARD.json").read_text(encoding="utf-8"))
    return (
        {row["id"]: row for row in tune["rows"]},
        {row["id"]: row for row in forward["rows"]},
    )


def main() -> None:
    assert_recipe_grid()
    fees = load_fees()
    payload = json.loads(INPUTS.read_text(encoding="utf-8"))
    days_by = _days_by_session(payload)
    store = CleanStore()

    def price(ticker: str, session: str, which: str):
        if which == "open":
            return store.session_open(ticker, session)
        if which == "close":
            return store.session_close(ticker, session)
        raise SystemExit(which)

    screen = _load_screen()
    cap_tune, cap_forward = _load_cap()
    checks: list[dict] = []
    recipes_out = []
    for spec in recipes():
        ident = spec["id"]
        starts = []
        anchor_book = None
        for start in START_DAYS:
            book = _walk(spec, _suffix(days_by, start), fees, price)
            if book["daily"][0]["session"] != start:
                raise SystemExit(f"{ident} started on {book['daily'][0]['session']}")
            starts.append(_start_row(book))
            if start == spec["anchor"]:
                anchor_book = book
            if spec["family"] == "v4" and start in SCREEN_STARTS:
                ref_id = f"v4:{ident}@{start}"
                if ref_id not in screen:
                    raise SystemExit(f"missing screen row {ref_id}")
                _compare_screen(checks, book, ident, start, screen[ref_id], "screen_monday")
            if spec["family"] == "g3" and start == spec["anchor"]:
                ref_id = f"g3:{ident}"
                if ref_id not in screen:
                    raise SystemExit(f"missing screen row {ref_id}")
                _compare_screen(checks, book, ident, start, screen[ref_id], "screen_g3")
            if spec["family"] == "cap_v3" and start == spec["anchor"]:
                tune_got = window_row(cap_slice(book, TUNE, CAPITAL))
                _compare_cap_window(
                    checks, tune_got, cap_tune[ident]["tune"], ident, start, "cap_tune",
                )
                p2_got = window_row(cap_slice(book, FORWARD, float(tune_got["end_equity"])))
                _compare_cap_window(
                    checks, p2_got, cap_forward[ident]["p2"], ident, start, "cap_p2",
                )
            if spec["family"] == "cap_v3" and start in SCREEN_STARTS:
                sessions = [day for day in TUNE if day >= start]
                part = window_row(cap_slice(book, sessions, CAPITAL))
                published = next(item for item in cap_tune[ident]["starts"] if item["start"] == start)
                for field in ("n", "win_rate", "up_share"):
                    _check(
                        checks,
                        group="cap_monday",
                        kind="count" if field == "n" else "return",
                        field=field,
                        got=part.get(field),
                        exp=published.get(field),
                        recipe=ident,
                        start=start,
                    )
        if anchor_book is None:
            raise SystemExit(f"missing anchor book {ident}")
        summary = summarize_starts(starts)
        if summary["n_starts"] != len(START_DAYS):
            raise SystemExit("start count")
        bad = _failed(checks, ident)
        row = {
            "anchor_start": spec["anchor"],
            "computed": not bad,
            "engine": "concentration_cap_v1.walk" if spec["engine"] == "cap" else "factor_mine_recipe_search_v4.walk keep_held",
            "family": spec["family"],
            "id": ident,
        }
        if bad:
            row["reason"] = "published book did not reproduce"
            print(ident, "NOT COMPUTED", len(bad), flush=True)
        else:
            row.update(summary)
            row["p2"] = summarize_p2(anchor_book["daily"], anchor_book["closed"])
            row["starts"] = starts
            print(
                ident,
                summary["positive"],
                "of",
                summary["n_starts"],
                "p2",
                row["p2"]["up"],
                row["p2"]["down"],
                row["p2"]["flat_sat"],
                flush=True,
            )
        recipes_out.append(row)
    body = {
        "capital": CAPITAL,
        "end_session": END_SESSION,
        "fee_path": "keep_held_futubull",
        "fill": "keep_held",
        "first_start": FIRST_START,
        "forward_hook": False,
        "frozen": False,
        "last_start": LAST_START,
        "luck_n": LUCK_N,
        "n_starts": len(START_DAYS),
        "new_tries": NEW_TRIES,
        "p2_sessions": list(P2),
        "picked": None,
        "recipes": recipes_out,
        "report_only": True,
        "sanity": {
            "cap_max_abs_diff": _max_diff(checks, group="cap_tune") if any(c["group"] == "cap_tune" for c in checks) else None,
            "checks": checks,
            "match_tol": MATCH_TOL,
            "max_abs_diff": _max_diff(checks),
            "screen_g3_max_abs_diff": _max_diff(checks, group="screen_g3", kind="return"),
            "screen_monday_max_abs_diff": _max_diff(checks, group="screen_monday", kind="return"),
        },
        "secondary_fee": "flat_15bp",
        "start_days": list(START_DAYS),
        "study": "start_day_sweep_v1",
    }
    # Cap max should cover tune, p2, and the Monday trade counts.
    cap_groups = {"cap_tune", "cap_p2", "cap_monday"}
    if any(row["group"] in cap_groups for row in checks):
        body["sanity"]["cap_max_abs_diff"] = max(
            float(row["abs_diff"]) for row in checks if row["group"] in cap_groups
        )
    text = render(body)
    RESULTS.write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")
    REPORT.write_text(text, encoding="utf-8")
    failed = [row["id"] for row in recipes_out if not row["computed"]]
    print("max screen monday diff", body["sanity"]["screen_monday_max_abs_diff"], flush=True)
    print("max all diff", body["sanity"]["max_abs_diff"], flush=True)
    if failed:
        raise SystemExit(f"not computed: {failed}")


if __name__ == "__main__":
    main()
