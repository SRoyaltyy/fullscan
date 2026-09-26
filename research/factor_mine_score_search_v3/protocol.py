"""Locked rules for factor_mine_score_search_v3.

v1 and v2 stay as committed. Luck N counts every formula, every book
size, and every start already walked there, plus the same cells here.
No window score lives here. A formula is not added or dropped after a result.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STUDY = Path(__file__).resolve().parent
PREREG = STUDY / "PREREG.md"
INPUTS = STUDY / "INPUTS.json"
DROP_PATH = STUDY / "DROP_LIST.json"
FREEZE = STUDY / "freeze" / "FREEZE.json"
RETURNS = STUDY / "returns"
REPORT = RETURNS / "REPORT.md"
MARKER = "<!-- BEGIN COVERED -->\n"

CAPITAL = 10_000.0
HARD_RED = -3.0
RANDOM4_SEED = 20260813
RANDOM4_DRAWS = 1000
RANDOM4_N = 4
TOP_X = (2, 4, 8)
FEATURES = ("cam", "yday", "rsi", "macd", "flow", "earn")

TUNE = (
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19",
    "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26",
    "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02",
    "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10",
    "2026-09-11",
)
FORWARD = (
    "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18",
    "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25",
)
SESSIONS = TUNE + FORWARD

CLEAN_COMMIT = "8e4683395de310e34261e48e9811e553c3a8621a"
CLEAN_BLOB = "e7bbac335cfa87243f9d0ddf8c331a0739dd0df8"
CLEAN_SHA256 = "5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2"
YAHOO_PATH = "data/prices/ohlc.parquet"
YAHOO_SHA256 = "559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55"
YAHOO_BLOB = "3456f7f489a6fa7033e8ae5cc942d8279f0113e3"

ALLOW_PREFIXES = (
    "research/factor_mine_score_search_v3/",
    ".github/workflows/factor_mine_score_search_v3.yml",
)

# v1 walked 34 formulas at X=2, 4, and 8 on one window that starts
# 2026-08-13. v2 walked the same 34 at those three sizes on 13 fresh
# starts. This study walks those 34 again on the same 13 starts.
# A strict report is not another formula, size, or start.
V1_FORMULAS = 34
V1_SIZES = 3
V1_STARTS = 1
V2_FORMULAS = 34
V2_SIZES = 3
V2_STARTS = 13
V3_FORMULAS = 34
V3_SIZES = 3
V3_STARTS = 13
V1_CELLS = V1_FORMULAS * V1_SIZES * V1_STARTS
V2_CELLS = V2_FORMULAS * V2_SIZES * V2_STARTS
V3_CELLS = V3_FORMULAS * V3_SIZES * V3_STARTS
LUCK_N = V1_CELLS + V2_CELLS + V3_CELLS
MIN_TRADES = 30
REJECT_JOINT = 0.5
RATIO_HI = 3.0
RATIO_LO = 1.0 / 3.0
SPLIT_TOL = 0.25

# Sessions through 2026-09-11 whose stored morning S is present and above -3.
# A red morning, including a missing S, does not buy. Max closed trades from
# a start is X times how many of these sessions fall on or after that start.
NON_RED = (
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-20", "2026-08-21",
    "2026-08-24", "2026-08-25", "2026-08-26", "2026-08-28", "2026-09-03",
    "2026-09-04", "2026-09-11",
)
RANK_STARTS = (
    "2026-08-20", "2026-08-21", "2026-08-25", "2026-08-26", "2026-08-27",
    "2026-08-28",
)
# The owner's short span. X=8 cannot reach 30 closed trades. Walked and
# flagged. Not in the rank key.
OBSERVE_STARTS = (
    "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09",
    "2026-09-10", "2026-09-11",
)

# Filled after INPUTS.json is written. The prereg repeats this hex.
INPUTS_SHA256 = "d7e2b96f35b968eab2c902aacfa8e25dc3bba1adf1bd59c93484d3f96645792b"
LABEL = "assumed pre-open, not server-proven"
FULLSCAN_BLOB = "832eaa4fdc19d368d87a4dad5c3edd3c200b0a54"
FULLSCAN_SHA256 = "1bfafd9d4f9a512b9bc3ac33d6d087349028f57ec4d0fedffe76ee0998e48f75"
EXCEL_SHA256 = "04e6b996c450427ea408646b305be91c33f0778c1f6ee167de9be1a08d129ded"


def _weights(**values: float) -> dict[str, float]:
    out = {name: 0.0 for name in FEATURES}
    out.update(values)
    return out


def _formula(
    fid: str,
    weights: dict[str, float] | None = None,
    *,
    kind: str = "blend",
    cap_yday: float | None = None,
    cap_macd: float | None = None,
    rsi_mode: str = "os",
) -> dict:
    return {
        "cap_macd": cap_macd,
        "cap_yday": cap_yday,
        "id": fid,
        "kind": kind,
        "rsi_mode": rsi_mode,
        "weights": weights or _weights(),
    }


def _all(**extra: float) -> dict[str, float]:
    base = _weights(cam=1, yday=1, rsi=1, macd=1, flow=1, earn=1)
    base.update(extra)
    return base


def build_grid() -> list[dict]:
    """34 formulas. The first is the board's current Score.

    The board Score in ``src/factor_mine_sim.py`` ``rank_score`` is
    ``100 - src_rank`` when the rank key is ``list`` or ``score``.
    The other points turn the morning columns on or off, change a
    weight, or cap yesterday's move and MACD histogram before the
    within-morning z-score.
    """
    grid = [_formula("board_list", kind="board_list")]
    for name in FEATURES:
        grid.append(_formula(f"only_{name}", _weights(**{name: 1})))
    grid.append(_formula("all_equal", _all()))
    for name in FEATURES:
        weights = _all()
        weights[name] = 0.0
        grid.append(_formula(f"drop_{name}", weights))
    pairs = (
        ("cam", "yday"), ("cam", "rsi"), ("cam", "macd"), ("cam", "flow"),
        ("cam", "earn"), ("yday", "rsi"), ("yday", "macd"), ("rsi", "macd"),
        ("flow", "earn"),
    )
    for left, right in pairs:
        grid.append(_formula(f"{left}_{right}", _weights(**{left: 1, right: 1})))
    for name in ("cam", "yday", "rsi", "macd"):
        grid.append(_formula(f"w2_{name}", _all(**{name: 2})))
    grid.append(_formula("cap_yday_10", _all(), cap_yday=10))
    grid.append(_formula("cap_yday_20", _all(), cap_yday=20))
    grid.append(_formula("cap_macd_0_5", _all(), cap_macd=0.5))
    grid.append(_formula("cap_macd_2", _all(), cap_macd=2))
    grid.append(_formula("cap_yday_10_macd_0_5", _all(), cap_yday=10, cap_macd=0.5))
    rev = _all()
    rev["yday"] = -1
    grid.append(_formula("yday_rev", rev))
    grid.append(_formula("rsi_mom", _all(), rsi_mode="mom"))
    ids = [row["id"] for row in grid]
    if len(grid) != 34 or len(set(ids)) != 34:
        raise SystemExit(f"grid size {len(grid)}")
    return grid


GRID = build_grid()
BY_ID = {row["id"]: row for row in GRID}


def prereg_fingerprint(text: str | None = None) -> str:
    raw = PREREG.read_text(encoding="utf-8") if text is None else text
    if MARKER not in raw:
        raise SystemExit("prereg marker missing")
    body = raw.split(MARKER, 1)[1]
    if not body.endswith("\n"):
        raise SystemExit("prereg body must end in a newline")
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_drop() -> dict:
    payload = json.loads(DROP_PATH.read_text(encoding="utf-8"))
    dropped = list(payload["dropped"])
    if dropped != sorted(set(dropped)) or len(dropped) != 77:
        raise SystemExit("drop list")
    if payload["matched_splits"] != ["ALP", "NFE", "TNMG", "WCT"]:
        raise SystemExit("matched splits")
    if payload["explained_split_still_dropped"] != ["YAAS"]:
        raise SystemExit("YAAS")
    return payload


def load_inputs() -> dict:
    if file_sha256(INPUTS) != INPUTS_SHA256:
        raise SystemExit("INPUTS.json sha mismatch")
    payload = json.loads(INPUTS.read_text(encoding="utf-8"))
    if payload.get("label") != LABEL:
        raise SystemExit("input label")
    if payload.get("fullscan_sha256") != FULLSCAN_SHA256:
        raise SystemExit("fullscan pin")
    if payload.get("excel_sha256") != EXCEL_SHA256:
        raise SystemExit("excel pin")
    dates = list(payload["sessions"])
    if dates != list(SESSIONS):
        raise SystemExit("input dates")
    if list(payload["dates"]) != dates:
        raise SystemExit("input date keys")
    return payload


def compound(returns: list[float]) -> float:
    acc = 1.0
    for value in returns:
        acc *= 1.0 + float(value)
    return acc - 1.0


def mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def median(values: list[float]) -> float | None:
    clean = sorted(float(value) for value in values)
    if not clean:
        return None
    mid = len(clean) // 2
    if len(clean) % 2:
        return clean[mid]
    return (clean[mid - 1] + clean[mid]) / 2.0


def day_counts(returns: list[float]) -> tuple[int, int, int]:
    up = down = flat = 0
    for value in returns:
        if value > 0:
            up += 1
        elif value < 0:
            down += 1
        else:
            flat += 1
    return up, down, flat


def zscores(values: list[float | None]) -> list[float]:
    finite = [float(value) for value in values if value is not None]
    if len(finite) < 2:
        return [0.0 for _ in values]
    mu = sum(finite) / len(finite)
    var = sum((value - mu) ** 2 for value in finite) / len(finite)
    if var <= 0.0:
        return [0.0 for _ in values]
    scale = var ** 0.5
    return [0.0 if value is None else (float(value) - mu) / scale for value in values]


def _clip(value: float, cap: float | None) -> float:
    if cap is None:
        return value
    return max(-float(cap), min(float(cap), value))


def raw_feature(row: dict, feature: str, formula: dict) -> float | None:
    if feature == "cam":
        if row.get("cond_good") is None and row.get("cond_bad") is None:
            return None
        return float(row.get("cond_good") or 0) - float(row.get("cond_bad") or 0)
    if feature == "yday":
        value = row.get("yday")
        if value is None:
            return None
        return _clip(float(value), formula.get("cap_yday"))
    if feature == "rsi":
        value = row.get("rsi")
        if value is None:
            return None
        if formula.get("rsi_mode") == "mom":
            return float(value) - 50.0
        return 50.0 - float(value)
    if feature == "macd":
        value = row.get("macd")
        if value is None:
            return None
        return _clip(float(value), formula.get("cap_macd"))
    if feature == "flow":
        if row.get("flow") is None:
            return None
        return 1.0 if row.get("flow") else 0.0
    if feature == "earn":
        pol = row.get("e_pol")
        if pol is None:
            return None
        if str(pol) == "good":
            return 1.0
        if str(pol) == "bad":
            return -1.0
        return 0.0
    raise SystemExit(feature)


def name_scores(rows: list[dict], formula: dict) -> list[tuple[float, dict]]:
    if formula["kind"] == "board_list":
        out = []
        for row in rows:
            src = 99 if row.get("src_rank") is None else int(row["src_rank"])
            out.append((float(100 - src), row))
        return out
    columns: dict[str, list[float]] = {}
    for feature, weight in formula["weights"].items():
        if not weight:
            continue
        columns[feature] = zscores([raw_feature(row, feature, formula) for row in rows])
    scored = []
    for index, row in enumerate(rows):
        total = 0.0
        for feature, weight in formula["weights"].items():
            if not weight:
                continue
            total += float(weight) * columns[feature][index]
        scored.append((total, row))
    return scored


def top_names(rows: list[dict], formula: dict, top_n: int) -> list[str]:
    """Highest score first. A missing open cannot take a slot. Ties: ticker."""
    ranked = name_scores(rows, formula)
    ranked.sort(key=lambda item: (-item[0], item[1]["ticker"]))
    picks = []
    for _score, row in ranked:
        if row.get("open") is None or float(row["open"]) <= 0:
            continue
        picks.append(row["ticker"])
        if len(picks) >= top_n:
            break
    return picks


def best_ticker(totals: dict[str, float], first: dict[str, str]) -> str | None:
    if not totals:
        return None
    return min(totals, key=lambda ticker: (-totals[ticker], first.get(ticker, "9999-99-99"), ticker))


def ex_best_compound(
    sessions: list[str],
    check: list[str],
    returns: list[float],
    pnl_by_day: dict[str, dict[str, float]],
    best: str | None,
) -> float | None:
    if not check or best is None or len(returns) != len(sessions):
        return None
    equity = CAPITAL
    by_session = dict(zip(sessions, returns))
    out: list[float] = []
    check_set = set(check)
    for session in sessions:
        start = equity
        end = start * (1.0 + float(by_session[session]))
        if session in check_set and start != 0.0:
            removed = float(pnl_by_day.get(session, {}).get(best) or 0.0)
            out.append((end - removed) / start - 1.0)
        equity = end
    if not out:
        return None
    return compound(out)


def simulate(days: list[dict], picks: dict[str, list[str]], fee_fn) -> dict:
    """Buy the open, sell the close. A missing close is carried to the next open.

    ``days`` is the full walk in order. ``picks`` is empty on a sit.
    """
    cash = CAPITAL
    lots: dict[str, dict] = {}
    prev = CAPITAL
    out_days = []
    pnl_by_day: dict[str, dict[str, float]] = {}
    first: dict[str, str] = {}
    for day in days:
        session = day["session"]
        by_ticker = {row["ticker"]: row for row in day["rows"]}
        sold = []
        bought = []
        for ticker in list(lots):
            lot = lots[ticker]
            row = by_ticker.get(ticker) or {}
            px = row.get("open")
            if px is None or float(px) <= 0:
                continue
            px = float(px)
            shares = lot["shares"]
            fee = float(fee_fn(shares, px, "sell"))
            cash += shares * px - fee
            pnl = shares * (px - lot["last"]) - fee
            pnl_by_day.setdefault(session, {})[ticker] = pnl_by_day.get(session, {}).get(ticker, 0.0) + pnl
            sold.append({"ticker": ticker, "shares": shares, "price": px, "pnl": shares * (px - lot["entry"]) - lot["fee_in"] - fee})
            lots.pop(ticker)
        chosen = [ticker for ticker in picks.get(session) or [] if ticker not in lots]
        if chosen and cash > 0:
            plan = _buy_plan(chosen, by_ticker, cash, fee_fn)
            for ticker, shares, px, fee in plan:
                cash -= shares * px + fee
                lots[ticker] = {"shares": shares, "entry": px, "fee_in": fee, "last": px, "entry_session": session}
                first.setdefault(ticker, session)
                bought.append({"ticker": ticker, "shares": shares, "price": px})
                pnl_by_day.setdefault(session, {})[ticker] = pnl_by_day.get(session, {}).get(ticker, 0.0) - fee
        for ticker in list(lots):
            lot = lots[ticker]
            row = by_ticker.get(ticker) or {}
            close = row.get("close")
            if close is None or float(close) <= 0:
                continue
            close = float(close)
            shares = lot["shares"]
            fee = float(fee_fn(shares, close, "sell"))
            cash += shares * close - fee
            delta = shares * (close - lot["last"]) - fee
            pnl_by_day.setdefault(session, {})[ticker] = pnl_by_day.get(session, {}).get(ticker, 0.0) + delta
            sold.append({
                "ticker": ticker,
                "shares": shares,
                "price": close,
                "pnl": shares * (close - lot["entry"]) - lot["fee_in"] - fee,
            })
            lots.pop(ticker)
        equity = cash
        for lot in lots.values():
            equity += lot["shares"] * float(lot["last"])
        ret = (equity / prev - 1.0) if prev else 0.0
        out_days.append({
            "bought": bought,
            "equity": equity,
            "ret": ret,
            "session": session,
            "sold": sold,
        })
        prev = equity
    return {"days": out_days, "first": first, "pnl_by_day": pnl_by_day}


def _buy_plan(tickers: list[str], by_ticker: dict, cash: float, fee_fn) -> list[tuple]:
    remaining = list(tickers)
    while remaining:
        per = cash / len(remaining)
        plan = []
        skipped = []
        for ticker in remaining:
            px = float(by_ticker[ticker]["open"])
            shares = _shares(per, px, fee_fn)
            if shares < 1:
                skipped.append(ticker)
                continue
            fee = float(fee_fn(shares, px, "buy"))
            plan.append((ticker, shares, px, fee))
        if not skipped:
            return plan
        affordable = [ticker for ticker in remaining if ticker not in skipped]
        if not affordable:
            return []
        remaining = affordable
    return []


def _shares(budget: float, px: float, fee_fn) -> int:
    if px <= 0 or budget <= 0:
        return 0
    trial = int(budget // px)
    while trial >= 1:
        fee = float(fee_fn(trial, px, "buy"))
        if trial * px + fee <= budget + 1e-6:
            return trial
        trial -= 1
    return 0


def window_stats(book: dict, sessions: list[str], window: tuple[str, ...]) -> dict:
    by_ret = {day["session"]: float(day["ret"]) for day in book["days"]}
    returns = [by_ret[session] for session in sessions]
    check = [by_ret[session] for session in window]
    pnl = book["pnl_by_day"]
    totals: dict[str, float] = {}
    for session in window:
        for ticker, value in pnl.get(session, {}).items():
            totals[ticker] = totals.get(ticker, 0.0) + float(value)
    best = best_ticker(totals, book["first"])
    closed = []
    entries = 0
    entered = 0
    for day in book["days"]:
        if day["session"] not in window:
            continue
        if day["bought"]:
            entered += 1
            entries += len(day["bought"])
        closed.extend(day["sold"])
    wins = [row for row in closed if row["pnl"] > 0]
    up, down, flat = day_counts(check)
    traded = 0
    for day in book["days"]:
        if day["session"] not in set(window):
            continue
        if day["bought"] or day["sold"]:
            traded += 1
    return {
        "best": best,
        "closed": len(closed),
        "compound": compound(check) if check else 0.0,
        "days_entered": entered,
        "days_traded": traded,
        "down": down,
        "entries": entries,
        "ex_best": ex_best_compound(list(sessions), list(window), returns, pnl, best),
        "flat": flat,
        "pnl": totals,
        "too_few": len(closed) < MIN_TRADES,
        "up": up,
        "up_share": (up / traded) if traded else None,
        "win_rate": (len(wins) / len(closed)) if closed else None,
    }


def hard_red(score) -> bool:
    if score is None:
        return True
    return float(score) <= HARD_RED


def non_red_from(start: str) -> int:
    return sum(1 for session in NON_RED if session >= start and session <= TUNE[-1])


def required_cells() -> tuple[tuple[str, int], ...]:
    """Cells where X times the non-red sessions can reach 30 trades.

    X=2 never can (12 non-red sessions times 2 is 24). X=4 can from
    2026-08-20 (9 times 4 is 36) and 2026-08-21 (8 times 4 is 32). X=8
    can from each rank start.
    """
    cells = []
    for start in RANK_STARTS:
        for top_n in TOP_X:
            if top_n * non_red_from(start) >= MIN_TRADES:
                cells.append((start, top_n))
    return tuple(cells)


def forward_status(closed: int, win_rate, up_share) -> str:
    """09-14..09-25 can reject. Under 30 closed trades cannot prove.

    The frozen id list does not change. ``rejected`` needs at least 30
    closed trades and a joint metric strictly below one half. ``proven``
    needs at least 30 and a joint metric at least one half. Anything
    shorter is ``unproven``.
    """
    joint = joint_metric(win_rate, up_share, closed)
    if joint is None:
        return "unproven"
    if joint < REJECT_JOINT:
        return "rejected"
    return "proven"


def joint_metric(win_rate, up_share, closed: int) -> float | None:
    """Worse of trade win rate and winning-day share. Under 30 is missing."""
    if closed < MIN_TRADES or win_rate is None or up_share is None:
        return None
    return min(float(win_rate), float(up_share))


def score_formula(cells: dict[tuple[str, int], dict]) -> dict:
    """One formula. ``cells`` maps (start, X) to closed, win_rate, up_share.

    Eligible means every required cell has at least 30 closed trades.
    The rank key is the minimum joint metric across those cells.
    """
    required = required_cells()
    joints = []
    eligible = True
    worst = None
    for pair in required:
        stats = cells.get(pair) or {}
        joint = joint_metric(stats.get("win_rate"), stats.get("up_share"), int(stats.get("closed") or 0))
        if joint is None:
            eligible = False
            continue
        joints.append(joint)
        if worst is None or joint < worst[0] or (joint == worst[0] and pair < worst[1]):
            worst = (joint, pair)
    return {
        "eligible": eligible,
        "mean_joint": mean(joints),
        "n_required": len(required),
        "n_required_cleared": len(joints),
        "rank_key": (min(joints) if joints else None),
        "worst": None if worst is None else {"joint": worst[0], "start": worst[1][0], "x": worst[1][1]},
    }


def rank_formulas(rows: list[dict]) -> list[str]:
    """Eligible formulas first. Then the worse metric, then its mean, then id.

    ``rows`` items need ``id`` plus the fields from ``score_formula``.
    An ineligible formula stays in the list and sorts after every eligible one.
    """
    def key(row: dict) -> tuple:
        rank_key = row.get("rank_key")
        mean_joint = row.get("mean_joint")
        return (
            0 if row.get("eligible") else 1,
            float("inf") if rank_key is None else -float(rank_key),
            float("inf") if mean_joint is None else -float(mean_joint),
            -int(row.get("n_required_cleared") or 0),
            row["id"],
        )

    return [row["id"] for row in sorted(rows, key=key)]


def split_explains(ratio: float, split: float | None) -> bool:
    if split is None or float(split) <= 0.0:
        return False
    for candidate in (float(split), 1.0 / float(split)):
        if candidate <= 0.0:
            continue
        if abs(float(ratio) - candidate) / candidate <= SPLIT_TOL:
            return True
    return False


def unexplained_jumps(bars: dict[str, list[dict]], splits: dict[tuple[str, str], float]) -> list[dict]:
    """Open/previous-close and close/open beyond 3x, unless a split explains it."""
    flags = []
    for ticker in sorted(bars):
        prev = None
        for row in bars[ticker]:
            legs = []
            if prev is not None and prev["close"] > 0 and row["open"] > 0:
                legs.append(("open_over_prev_close", row["open"] / prev["close"]))
            if row["open"] > 0 and row["close"] > 0:
                legs.append(("close_over_open", row["close"] / row["open"]))
            for leg, ratio in legs:
                if ratio > RATIO_HI or ratio < RATIO_LO:
                    split = splits.get((ticker, row["date"]))
                    if not split_explains(ratio, split):
                        flags.append({
                            "date": row["date"],
                            "leg": leg,
                            "ratio": round(float(ratio), 6),
                            "ticker": ticker,
                        })
            prev = row
    return flags


def active_inputs(formula: dict) -> dict[str, float | str]:
    if formula["kind"] == "board_list":
        return {"board_list": 1}
    out: dict[str, float | str] = {}
    for feature, weight in formula["weights"].items():
        if weight:
            out[feature] = weight
    if formula.get("cap_yday") is not None:
        out["cap_yday"] = formula["cap_yday"]
    if formula.get("cap_macd") is not None:
        out["cap_macd"] = formula["cap_macd"]
    if formula.get("rsi_mode") == "mom":
        out["rsi_mode"] = "mom"
    return out
