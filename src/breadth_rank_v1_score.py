"""Score breadth_rank_v1 one session at a time.

Train loads no bar dated after 2026-09-13 and writes no session after
2026-09-11. The frozen rank is that result. Test appends later sessions
and does not refit.
"""
from __future__ import annotations

import argparse
import json
import subprocess
from bisect import bisect_left
from pathlib import Path

import numpy as np

from src.breadth_rank_v1_append import file_sha256, manifest_line
from src.breadth_rank_v1_bars import load_bars
from src.breadth_rank_v1_grid import (
    ATOMS,
    BASKETS,
    HOLD,
    LUCK_DENOMINATOR,
    MIN_CLOSED,
    MIN_HOLD,
    N,
    RANDOM4_DRAWS,
    RANDOM4_N,
    RANDOM4_SEED,
    WIN_MIN,
    iter_rules,
    rule_id,
)
from src.breadth_rank_v1_inputs import (
    ab_map,
    actions_map,
    book_map,
    catalyst_names,
    finviz_map,
    git_blob,
    heat_map,
    judge_map,
    panel_names,
    parse_predict,
    sector_slug,
)
from src.breadth_rank_v1_metrics import (
    asymmetric_payoff,
    best_ticker,
    compound,
    day_counts,
    ex_best_compound,
    luck_p,
    raw_luck_p,
    start_day_win_rate,
    top_share,
)
from src.breadth_rank_v1_protocol import (
    BAR_BLOB_SHA,
    BAR_PATH,
    BAR_SHA256,
    FIT_CUTOFF,
    FROZEN_PATH,
    LAST_SESSION,
    OOS_START,
    RANK_END,
    RANK_SESSIONS,
    RETURNS,
    ROOT,
    SESSIONS,
    STUDY,
    STUDY_LABEL,
    TEST_SESSIONS,
    assert_prereg,
    load_manifest,
)
from src.candle_factor import _from_bars as candle_from_bars
from src.factor_mine_book import HARD_RED
from src.factor_mine_combo import HARD_RED_SIT, hard_red_skip_new
from src.lever_search_bars import SameDayBarError
from src.ohlc_ripper import from_bars as ohlc_from_bars
from src.paper_trade import load_fees, order_fees

CAPITAL = 10_000.0
BORROW_DAY = 0.01 / 252.0
BP_SIDE = 0.000075
ATOM_INDEX = {name: i for i, name in enumerate(ATOMS)}


def red_day_sit(score: float | None, side: str) -> bool:
    """Factor Mine's hard-red guard. No new buys when S <= HARD_RED.

    The threshold is factor_mine_book.HARD_RED. The skip decision is
    factor_mine_combo.hard_red_skip_new in its default sit mode, which
    blocks both sides. A missing score does not sit.
    """
    if score is None:
        return False
    if float(score) > float(HARD_RED):
        return False
    return bool(hard_red_skip_new(side, HARD_RED_SIT))


def percentile_ranks(values: np.ndarray) -> np.ndarray:
    """Average rank divided by the count. Ties share the average rank."""
    n = int(values.shape[0])
    if n == 0:
        return values.astype(np.float64)
    order = np.argsort(values, kind="mergesort")
    ranks = np.empty(n, dtype=np.float64)
    ordered = values[order]
    i = 0
    while i < n:
        j = i + 1
        while j < n and ordered[j] == ordered[i]:
            j += 1
        ranks[order[i:j]] = (i + 1 + j) / 2.0 / n
        i = j
    return ranks


def fee_15(shares: int, price: float) -> float:
    if shares <= 0 or price <= 0:
        return 0.0
    return shares * price * BP_SIDE


class Book:
    __slots__ = ("c15", "cf", "eq15", "eqf", "pos")

    def __init__(self) -> None:
        self.cf = CAPITAL
        self.c15 = CAPITAL
        self.eqf = CAPITAL
        self.eq15 = CAPITAL
        self.pos: dict[str, dict] = {}


def want_exit(held: int) -> bool:
    """Time exit. The entry session is zero. Sell once held reaches the hold.

    min_hold equals hold, so nothing is sold earlier than the hold.
    """
    if held < MIN_HOLD:
        return False
    return held >= HOLD


def _buy_shares(cash: float, px: float, per: float, fees: dict) -> tuple[int, float]:
    shares = int(per // px) if px else 0
    while shares >= 1:
        fee_f = order_fees(shares, px, "buy", fees)
        if shares * px + fee_f <= cash + 1e-6:
            return shares, fee_f
        shares -= 1
    return 0, 0.0


def advance_book(book: Book, *, side: str, pick_names: list[str], open_of: dict[str, float],
                 close_of: dict[str, float], day_i: int, fees: dict,
                 final_session: bool) -> dict:
    """One session.

    A held name with no positive open is closed at its last marked price
    (`missing_bar`) and is not carried. On the last study session, anything
    still open after the open exits and the buys is closed at the close
    (`window_end`).
    """
    if not book.pos and not pick_names and not final_session:
        return {
            "entries": [],
            "exits": [],
            "n_entries": 0,
            "pnl": {},
            "ret_15": 0.0,
            "ret_f": 0.0,
            "trade_rets": [],
        }
    pnl: dict[str, float] = {}
    closed: list[float] = []
    exits: list[dict] = []
    entries: list[str] = []

    def close_lot(ticker: str, px: float, reason: str, charge_borrow: bool) -> None:
        lot = book.pos[ticker]
        shares = int(lot["shares"])
        prev_px = float(lot["prev"])
        exit_side = "sell" if side == "long" else "buy"
        fee_f = order_fees(shares, px, exit_side, fees)
        fee_b = fee_15(shares, px)
        borrow = shares * px * BORROW_DAY if (charge_borrow and side == "short") else 0.0
        if borrow:
            book.cf -= borrow
            book.c15 -= borrow
        if side == "long":
            book.cf += shares * px - fee_f
            book.c15 += shares * px - fee_b
            delta = shares * (px - prev_px) - fee_f
            trip = shares * (px - lot["entry_px"]) - lot["fee_in"] - fee_f
        else:
            book.cf -= shares * px + fee_f
            book.c15 -= shares * px + fee_b
            delta = shares * (prev_px - px) - fee_f - borrow
            trip = shares * (lot["entry_px"] - px) - lot["fee_in"] - fee_f - borrow
        if lot["entry_i"] == day_i:
            delta -= lot["fee_in"]
        pnl[ticker] = pnl.get(ticker, 0.0) + delta
        notional = shares * float(lot["entry_px"])
        ret = (trip / notional) if notional else 0.0
        closed.append(ret)
        exits.append({"reason": reason, "ret": round(ret, 6), "ticker": ticker})
        del book.pos[ticker]

    for ticker in sorted(book.pos):
        lot = book.pos[ticker]
        held = day_i - int(lot["entry_i"])
        px = open_of.get(ticker)
        if px is None or px <= 0:
            close_lot(ticker, float(lot["prev"]), "missing_bar", False)
            continue
        if not want_exit(held):
            continue
        close_lot(ticker, float(px), "time", False)

    if pick_names and (book.cf > 0 or side == "short"):
        new = [ticker for ticker in pick_names if ticker not in book.pos]
        if new:
            if side == "long":
                room = book.cf
            else:
                stock = 0.0
                for ticker, lot in book.pos.items():
                    px = open_of.get(ticker)
                    if px is None or px <= 0:
                        px = float(lot["prev"])
                    stock -= lot["shares"] * px
                room = max(0.0, (book.cf + stock) * 0.5)
            per = room / len(new) if new else 0.0
            for ticker in new:
                px = open_of.get(ticker)
                if px is None or px <= 0:
                    continue
                if side == "long":
                    shares, fee_f = _buy_shares(book.cf, px, per, fees)
                    if shares < 1:
                        continue
                    fee_b = fee_15(shares, px)
                    book.cf -= shares * px + fee_f
                    book.c15 -= shares * px + fee_b
                else:
                    shares = int(per // px) if px else 0
                    if shares < 1:
                        continue
                    fee_f = order_fees(shares, px, "sell", fees)
                    fee_b = fee_15(shares, px)
                    book.cf += shares * px - fee_f
                    book.c15 += shares * px - fee_b
                book.pos[ticker] = {
                    "entry_i": day_i,
                    "entry_px": px,
                    "extreme": px,
                    "fee_in": fee_f,
                    "prev": px,
                    "shares": shares,
                }
                entries.append(ticker)

    if final_session:
        for ticker in sorted(book.pos):
            lot = book.pos[ticker]
            px = close_of.get(ticker)
            if px is None or px <= 0:
                px = float(lot["prev"])
            close_lot(ticker, float(px), "window_end", True)

    for ticker in sorted(book.pos):
        lot = book.pos[ticker]
        close = close_of.get(ticker)
        if close is None or close <= 0:
            close = float(lot["prev"])
        prev_px = float(lot["prev"])
        borrow = lot["shares"] * close * BORROW_DAY if side == "short" else 0.0
        if borrow:
            book.cf -= borrow
            book.c15 -= borrow
        if side == "long":
            mtm = lot["shares"] * (close - prev_px)
        else:
            mtm = lot["shares"] * (prev_px - close) - borrow
        if lot["entry_i"] == day_i:
            mtm -= lot["fee_in"]
        if mtm:
            pnl[ticker] = pnl.get(ticker, 0.0) + mtm
        lot["prev"] = close

    stock_f = 0.0
    for lot in book.pos.values():
        notion = lot["shares"] * float(lot["prev"])
        stock_f += notion if side == "long" else -notion
    eqf = book.cf + stock_f
    eq15 = book.c15 + stock_f
    ret_f = (eqf / book.eqf - 1.0) if book.eqf else 0.0
    ret_15 = (eq15 / book.eq15 - 1.0) if book.eq15 else 0.0
    book.eqf = eqf
    book.eq15 = eq15
    return {
        "entries": entries,
        "exits": exits,
        "n_entries": len(entries),
        "pnl": pnl,
        "ret_15": ret_15,
        "ret_f": ret_f,
        "trade_rets": closed,
    }


class BarView:
    def __init__(self, rows: dict[str, list[dict]]) -> None:
        self.rows: dict[str, tuple[list[str], list[dict]]] = {}
        for ticker, bars in rows.items():
            ordered = sorted(bars, key=lambda bar: bar["date"])
            self.rows[str(ticker).upper()] = ([bar["date"] for bar in ordered], ordered)

    def session_open_close(self, ticker: str, session: str) -> tuple[float | None, float | None]:
        """D's open and close. D's high and low are not read."""
        rec = self.rows.get(ticker)
        if rec is None:
            return None, None
        dates, bars = rec
        index = bisect_left(dates, session)
        if index >= len(dates) or dates[index] != session:
            return None, None
        bar = bars[index]
        opx = float(bar["open"])
        cpx = float(bar["close"])
        if opx <= 0:
            opx_out = None
        else:
            opx_out = opx
        if cpx <= 0:
            cpx_out = None
        else:
            cpx_out = cpx
        return opx_out, cpx_out

    def prior_bars(self, ticker: str, session: str) -> list[dict]:
        rec = self.rows.get(ticker)
        if rec is None:
            return []
        dates, bars = rec
        end = bisect_left(dates, session)
        if end > 0 and dates[end - 1] >= session:
            raise SameDayBarError(f"same-day-or-later bar {dates[end - 1]} for session {session}")
        if end < len(dates) and dates[end] < session:
            raise SameDayBarError(f"same-day-or-later bar {dates[end]} for session {session}")
        start = max(0, end - 80)
        out = []
        for bar in bars[start:end]:
            if bar["date"] >= session:
                raise SameDayBarError(f"same-day-or-later bar {bar['date']} for session {session}")
            out.append({
                "close": float(bar["close"]),
                "date": bar["date"],
                "high": float(bar["high"]),
                "low": float(bar["low"]),
                "open": float(bar["open"]),
                "volume": float(bar["volume"]),
            })
        return out


def _blob(cache: dict[str, bytes], sha: str) -> bytes:
    blob = cache.get(sha)
    if blob is None:
        blob = git_blob(sha)
        cache[sha] = blob
    return blob


def build_day(view: BarView, rows: list[dict], session: str, cache: dict[str, bytes]) -> dict:
    by_kind: dict[str, list[dict]] = {}
    for row in rows:
        if row["date"] != session:
            raise SystemExit(f"input {row['path']} labelled {row['date']} used on {session}")
        by_kind.setdefault(row["input"], []).append(row)

    names: set[str] = set()
    buys: set[str] = set()
    sectors: dict[str, str] = {}
    if "stock_book" in by_kind:
        buy, named, sec = book_map(_blob(cache, by_kind["stock_book"][0]["blob_sha"]))
        buys |= buy
        names |= named
        sectors.update(sec)
    if "panel" in by_kind:
        row = by_kind["panel"][0]
        names.update(panel_names(_blob(cache, row["blob_sha"]), session, int(row["n_rows"])))
    catalyst: set[str] = set()
    if "catalyst" in by_kind:
        catalyst = catalyst_names(_blob(cache, by_kind["catalyst"][0]["blob_sha"]))
        names |= catalyst
    judge: dict[str, float] = {}
    if "judge" in by_kind:
        judge, named = judge_map(_blob(cache, by_kind["judge"][0]["blob_sha"]))
        names |= named
    actions: dict[str, float] = {}
    if "actions" in by_kind:
        actions, named = actions_map(_blob(cache, by_kind["actions"][0]["blob_sha"]))
        names |= named
    heat: dict[str, float] = {}
    if "heat" in by_kind:
        heat, named = heat_map(_blob(cache, by_kind["heat"][0]["blob_sha"]))
        names |= named

    tradable = []
    open_of: dict[str, float] = {}
    close_of: dict[str, float] = {}
    for ticker in sorted(names):
        opx, cpx = view.session_open_close(ticker, session)
        if opx is None:
            continue
        tradable.append(ticker)
        open_of[ticker] = opx
        if cpx is not None:
            close_of[ticker] = cpx

    week: dict[str, float] = {}
    if "export" in by_kind and tradable:
        week, fv_sectors = finviz_map(_blob(cache, by_kind["export"][0]["blob_sha"]), set(tradable))
        for ticker, sector in fv_sectors.items():
            sectors[ticker] = sector

    ab: dict[str, float] = {}
    if "ab_checklist" in by_kind and "ab_enriched" in by_kind:
        ab = ab_map(
            _blob(cache, by_kind["ab_checklist"][0]["blob_sha"]),
            _blob(cache, by_kind["ab_enriched"][0]["blob_sha"]),
        )

    score = None
    direction = None
    if "predict" in by_kind:
        text = _blob(cache, by_kind["predict"][0]["blob_sha"]).decode("utf-8", errors="replace")
        score, direction = parse_predict(text)
    if direction == "up":
        predict_value = 1.0
    elif direction == "down":
        predict_value = -1.0
    else:
        predict_value = 0.0
    market_score = float(score) if score is not None else 0.0

    sector_dir: dict[str, float] = {}
    if "sector" in by_kind:
        for row in by_kind["sector"]:
            slug = Path(row["path"]).name.replace("_predict.md", "")
            text = _blob(cache, row["blob_sha"]).decode("utf-8", errors="replace")
            _sc, direc = parse_predict(text)
            if direc == "up":
                sector_dir[slug] = 1.0
            elif direc == "down":
                sector_dir[slug] = -1.0
            else:
                sector_dir[slug] = 0.0

    n = len(tradable)
    raw = np.zeros((n, len(ATOMS)), dtype=np.float64)
    for i, ticker in enumerate(tradable):
        prior = view.prior_bars(ticker, session)
        feat = ohlc_from_bars(prior) if prior else {"hot_score": 0.0}
        hot = float(feat.get("hot_score") or 0.0)
        last_body = 0.0
        sma_gap = 0.0
        engulf = 0.0
        if prior:
            last = prior[-1]
            if last["open"] > 0:
                last_body = (last["close"] - last["open"]) / last["open"]
            if len(prior) >= 50:
                closes = [bar["close"] for bar in prior[-50:]]
                sma = sum(closes) / 50.0
                if sma > 0:
                    sma_gap = last["close"] / sma - 1.0
        if len(prior) >= 2:
            engulf = 1.0 if candle_from_bars(prior[-8:]).get("engulf_bull") else 0.0
        slug = sector_slug(sectors.get(ticker, ""))
        raw[i, ATOM_INDEX["a09_sma50"]] = sma_gap
        raw[i, ATOM_INDEX["ab_good"]] = float(ab.get(ticker, 0.0))
        raw[i, ATOM_INDEX["actions_good"]] = float(actions.get(ticker, 0.0))
        raw[i, ATOM_INDEX["book_buy"]] = 1.0 if ticker in buys else 0.0
        raw[i, ATOM_INDEX["catalyst_on"]] = 1.0 if ticker in catalyst else 0.0
        raw[i, ATOM_INDEX["cd_engulf_bull"]] = engulf
        raw[i, ATOM_INDEX["fv_week_pos"]] = float(week.get(ticker, 0.0))
        raw[i, ATOM_INDEX["hard_red"]] = market_score
        raw[i, ATOM_INDEX["heat_up"]] = float(heat.get(ticker, 0.0))
        raw[i, ATOM_INDEX["hot_pos"]] = hot
        raw[i, ATOM_INDEX["judge_up"]] = float(judge.get(ticker, 0.0))
        raw[i, ATOM_INDEX["last_green"]] = last_body
        raw[i, ATOM_INDEX["predict_up"]] = predict_value
        raw[i, ATOM_INDEX["s_gt_0"]] = market_score
        raw[i, ATOM_INDEX["sector_up"]] = float(sector_dir.get(slug, 0.0)) if slug else 0.0
    if n:
        raw = np.nan_to_num(raw, nan=0.0, posinf=0.0, neginf=0.0)

    columns = {atom: percentile_ranks(raw[:, ATOM_INDEX[atom]]) for atom in ATOMS} if n else {}
    order: dict[str, dict[str, list[str]]] = {}
    for basket, atoms in BASKETS:
        if n == 0:
            order[basket] = {"long": [], "short": []}
            continue
        score_v = np.zeros(n, dtype=np.float64)
        for atom in atoms:
            score_v += columns[atom]
        long_idx = sorted(range(n), key=lambda i: (-float(score_v[i]), tradable[i]))
        short_idx = sorted(range(n), key=lambda i: (float(score_v[i]), tradable[i]))
        order[basket] = {
            "long": [tradable[i] for i in long_idx],
            "short": [tradable[i] for i in short_idx],
        }

    blobs = [{"blob_sha": BAR_BLOB_SHA, "path": BAR_PATH}]
    for row in rows:
        blobs.append({"blob_sha": row["blob_sha"], "path": row["path"]})
    blobs = sorted(
        {(item["path"], item["blob_sha"]): item for item in blobs}.values(),
        key=lambda item: (item["path"], item["blob_sha"]),
    )
    return {
        "blobs": blobs,
        "close_of": close_of,
        "morning_s": score,
        "names": tradable,
        "open_of": open_of,
        "order": order,
        "session": session,
    }


def _ensure_held(day: dict, books: list[Book], view: BarView) -> None:
    needed = set()
    for book in books:
        needed.update(book.pos)
    for ticker in sorted(needed):
        if ticker in day["open_of"] and ticker in day["close_of"]:
            continue
        opx, cpx = view.session_open_close(ticker, day["session"])
        if opx is not None and ticker not in day["open_of"]:
            day["open_of"][ticker] = opx
        if cpx is not None and ticker not in day["close_of"]:
            day["close_of"][ticker] = cpx


def _day_bytes(session: str, morning_s: float | None, hard_red: bool, results: list[dict]) -> bytes:
    def packed(key: str, rounder):
        out = {}
        for i, result in enumerate(results):
            value = result[key]
            if not value:
                continue
            if key == "pnl":
                out[str(i)] = {ticker: round(float(pnl), 4) for ticker, pnl in sorted(value.items())}
            elif key == "trade_rets":
                out[str(i)] = [round(float(item), 6) for item in value]
            elif key == "exits":
                out[str(i)] = sorted(value, key=lambda item: (item["ticker"], item["reason"]))
            else:
                out[str(i)] = value
        return out

    payload = {
        "designed_after": True,
        "entries": packed("entries", None),
        "exits": packed("exits", None),
        "hard_red": hard_red,
        "label": STUDY_LABEL,
        "morning_s": None if morning_s is None else round(float(morning_s), 4),
        "n": N,
        "n_entries": [int(result["n_entries"]) for result in results],
        "pnl": packed("pnl", None),
        "ret_flat_15bp": [round(float(result["ret_15"]), 8) for result in results],
        "ret_futubull": [round(float(result["ret_f"]), 8) for result in results],
        "session": session,
        "study": STUDY,
        "trade_rets": packed("trade_rets", None),
    }
    return (json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")


def _write_or_match(path: Path, raw: bytes) -> None:
    if path.is_file():
        old = path.read_bytes()
        if old != raw:
            raise SystemExit(f"existing per-day file changed ({path.name}); earlier return changed")
        return
    path.write_bytes(raw)


def _pin_inputs(index: dict) -> None:
    seen = set()
    for row in index.values():
        key = (row["commit"], row["path"])
        if key in seen:
            continue
        seen.add(key)
        got = subprocess.check_output(
            ["git", "rev-parse", f"{row['commit']}:{row['path']}"],
            cwd=ROOT,
            text=True,
        ).strip()
        if got != row["blob_sha"]:
            raise SystemExit(f"pin blob mismatch {row['path']} {row['commit']}")


def _verify_bar_blob() -> None:
    from src.breadth_rank_v1_protocol import (
        DROPPED_BLOB_SHA,
        DROPPED_PATH,
        SPLIT_BLOB_SHA,
        SPLIT_PATH,
        file_sha256,
    )
    blob = subprocess.check_output(["git", "hash-object", BAR_PATH], cwd=ROOT, text=True).strip()
    if blob != BAR_BLOB_SHA:
        raise SystemExit("pinned bar blob mismatch")
    if file_sha256(ROOT / BAR_PATH) != BAR_SHA256:
        raise SystemExit("pinned bar sha256 mismatch")
    for rel, pin in ((SPLIT_PATH, SPLIT_BLOB_SHA), (DROPPED_PATH, DROPPED_BLOB_SHA)):
        got = subprocess.check_output(["git", "hash-object", rel], cwd=ROOT, text=True).strip()
        if got != pin:
            raise SystemExit(f"pinned blob mismatch {rel}")


def _load_view(phase: str) -> BarView:
    if phase == "train":
        rows = load_bars(max_date=FIT_CUTOFF)
        for bars in rows.values():
            for bar in bars:
                if bar["date"] >= OOS_START:
                    raise SystemExit(f"train loaded {bar['date']}")
        return BarView(rows)
    if phase == "test":
        if not FROZEN_PATH.is_file():
            raise SystemExit("frozen rank is missing; test days are not scored")
        return BarView(load_bars())
    raise SystemExit(f"unknown phase {phase}")


def _sessions(phase: str) -> tuple[str, ...]:
    if phase == "train":
        for session in TEST_SESSIONS:
            if (RETURNS / f"{session}.json").is_file():
                raise SystemExit("test day already scored")
        return RANK_SESSIONS
    return SESSIONS


def walk_phase(phase: str) -> None:
    if N > 50:
        raise SystemExit("candidate list above 50")
    if len(list(iter_rules())) != N:
        raise SystemExit("rule count drifted")
    assert_prereg()
    _verify_bar_blob()
    _manifest, index = load_manifest()
    _pin_inputs(index)
    grouped: dict[str, list[dict]] = {day: [] for day in SESSIONS}
    for row in index.values():
        grouped[row["date"]].append(row)
    view = _load_view(phase)
    fees = load_fees()
    rules = list(iter_rules())
    books = [Book() for _ in rules]
    cache: dict[str, bytes] = {}
    lines = []
    RETURNS.mkdir(parents=True, exist_ok=True)
    sessions = _sessions(phase)
    # Test recomputes the rank window first so the book matches, then appends.
    run_sessions = SESSIONS if phase == "test" else sessions
    for day_i, session in enumerate(run_sessions):
        if phase == "train" and session > RANK_END:
            raise SystemExit("train walked a test session")
        if phase == "train" and session >= OOS_START:
            raise SystemExit("train walked an OOS session")
        print(f"walk {session}", flush=True)
        day = build_day(view, grouped.get(session) or [], session, cache)
        _ensure_held(day, books, view)
        hard = red_day_sit(day["morning_s"], "long")
        final = phase == "test" and session == LAST_SESSION
        results = []
        for rule in rules:
            _index, rid, basket, _atoms, side, top_n = rule
            if rule_id(basket, side, top_n) != rid:
                raise SystemExit("rule id drift")
            sit = red_day_sit(day["morning_s"], side)
            if sit != hard:
                raise SystemExit("red-day guard disagreed across sides")
            picks = [] if sit else day["order"][basket][side][:top_n]
            results.append(advance_book(
                books[rule[0]],
                side=side,
                pick_names=picks,
                open_of=day["open_of"],
                close_of=day["close_of"],
                day_i=day_i,
                fees=fees,
                final_session=final,
            ))
        raw = _day_bytes(session, day["morning_s"], hard, results)
        path = RETURNS / f"{session}.json"
        _write_or_match(path, raw)
        lines.append(manifest_line({
            "file": path.name,
            "input_blobs": day["blobs"],
            "session": session,
            "sha256": file_sha256(raw),
        }))
    _write_manifest(lines)
    if phase == "train":
        _write_freeze()
    else:
        _write_report(view, fees)


def _write_manifest(lines: list[str]) -> None:
    text = ("\n".join(lines) + "\n") if lines else ""
    path = RETURNS / "manifest.jsonl"
    if not path.is_file():
        path.write_text(text, encoding="utf-8")
        return
    old = path.read_text(encoding="utf-8")
    if old == text or text.startswith(old):
        if old != text:
            path.write_text(text, encoding="utf-8")
        return
    raise SystemExit("manifest would change earlier lines")


def _load_recorded(sessions: tuple[str, ...] | list[str]) -> list[dict]:
    out = []
    for session in sessions:
        path = RETURNS / f"{session}.json"
        if not path.is_file():
            raise SystemExit(f"missing day file {session}")
        out.append(json.loads(path.read_text(encoding="utf-8")))
    return out


def _series(days: list[dict]) -> dict:
    sessions = [day["session"] for day in days]
    n = N
    ret_f = []
    ret_15 = []
    entries = []
    pnl = []
    trades = []
    first: list[dict[str, str]] = [{} for _ in range(n)]
    for day in days:
        ret_f.append(day["ret_futubull"])
        ret_15.append(day["ret_flat_15bp"])
        entries.append(day["n_entries"])
        day_pnl = []
        day_tr = []
        for i in range(n):
            key = str(i)
            day_pnl.append({ticker: float(value) for ticker, value in (day.get("pnl") or {}).get(key, {}).items()})
            day_tr.append([float(value) for value in (day.get("trade_rets") or {}).get(key, [])])
            for ticker in (day.get("entries") or {}).get(key, []):
                first[i].setdefault(ticker, day["session"])
        pnl.append(day_pnl)
        trades.append(day_tr)
    columns_f = [[ret_f[d][i] for d in range(len(days))] for i in range(n)]
    columns_15 = [[ret_15[d][i] for d in range(len(days))] for i in range(n)]
    columns_ent = [[entries[d][i] for d in range(len(days))] for i in range(n)]
    columns_pnl = [[pnl[d][i] for d in range(len(days))] for i in range(n)]
    columns_tr = [[trades[d][i] for d in range(len(days))] for i in range(n)]
    return {
        "entries": columns_ent,
        "first": first,
        "pnl": columns_pnl,
        "ret_15": columns_15,
        "ret_f": columns_f,
        "sessions": sessions,
        "trades": columns_tr,
    }


def _slice(series: dict, rule_index: int, check: list[str]) -> dict:
    sessions = series["sessions"]
    rets = series["ret_f"][rule_index]
    rets15 = series["ret_15"][rule_index]
    check_set = set(check)
    pnl_by_day = {}
    totals: dict[str, float] = {}
    trade_list: list[float] = []
    entries = 0
    days_entered = 0
    check_rets = []
    check_rets15 = []
    for i, session in enumerate(sessions):
        if session not in check_set:
            continue
        check_rets.append(rets[i])
        check_rets15.append(rets15[i])
        day_pnl = series["pnl"][rule_index][i]
        if day_pnl:
            pnl_by_day[session] = day_pnl
            for ticker, value in day_pnl.items():
                totals[ticker] = totals.get(ticker, 0.0) + value
        trade_list.extend(series["trades"][rule_index][i])
        n_ent = int(series["entries"][rule_index][i])
        entries += n_ent
        if n_ent:
            days_entered += 1
    first = series["first"][rule_index]
    best = best_ticker(totals, first)
    share, best_name = top_share(totals, first)
    wins = sum(1 for value in trade_list if value > 0)
    up, down, flat = day_counts(check_rets)
    return {
        "asymmetric": asymmetric_payoff(trade_list),
        "best": best_name,
        "closed": len(trade_list),
        "compound": compound(check_rets) if check_rets else 0.0,
        "compound_15": compound(check_rets15) if check_rets15 else 0.0,
        "days_entered": days_entered,
        "down": down,
        "entries": entries,
        "ex_best": ex_best_compound(sessions, [session for session in sessions if session in check_set], rets, pnl_by_day, best),
        "flat": flat,
        "start_day_win": start_day_win_rate(check_rets),
        "top_share": share,
        "up": up,
        "win_rate": (wins / len(trade_list)) if trade_list else None,
    }


def _round_opt(value, digits: int = 8):
    if value is None:
        return None
    return round(float(value), digits)


def freeze_payload(series: dict) -> dict:
    rules = list(iter_rules())
    ranked = []
    untestable = []
    for index, rid, _basket, _atoms, _side, _top in rules:
        stats = _slice(series, index, list(RANK_SESSIONS))
        row = {
            "closed_trades": stats["closed"],
            "compound": _round_opt(stats["compound"]),
            "entries": stats["entries"],
            "ex_best": _round_opt(stats["ex_best"]),
            "id": rid,
            "win_rate": _round_opt(stats["win_rate"]),
        }
        if stats["entries"] <= 0:
            row["label"] = "untestable"
            untestable.append(row)
            continue
        row["label"] = "too few trades" if stats["closed"] < MIN_CLOSED else "ranked"
        ranked.append(row)
    ranked.sort(key=lambda row: (
        row["ex_best"] is None,
        -(row["ex_best"] if row["ex_best"] is not None else 0.0),
        -(row["compound"] if row["compound"] is not None else 0.0),
        row["id"],
    ))
    for i, row in enumerate(ranked, start=1):
        row["rank"] = i
    rank1 = ranked[0]["id"] if ranked else None
    label = ranked[0]["label"] if ranked else "untestable"
    closed = ranked[0]["closed_trades"] if ranked else 0
    return {
        "fit_cutoff": FIT_CUTOFF,
        "nothing_fitted_after": FIT_CUTOFF,
        "rank1": rank1,
        "rank1_closed_trades": closed,
        "rank1_label": label,
        "rank_end": RANK_END,
        "rows": ranked,
        "study": STUDY,
        "untestable": untestable,
    }


def _write_freeze() -> None:
    series = _series(_load_recorded(RANK_SESSIONS))
    payload = freeze_payload(series)
    raw = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    if FROZEN_PATH.is_file() and FROZEN_PATH.read_bytes() != raw:
        raise SystemExit("frozen rank would change")
    if not FROZEN_PATH.is_file():
        FROZEN_PATH.write_bytes(raw)
    print(f"rank1 {payload['rank1']} {payload['rank1_label']} closed {payload['rank1_closed_trades']}", flush=True)


def _baseline_returns(view: BarView, fees: dict, days: list[dict]) -> tuple[list[float], list[list[float]]]:
    import random

    iwm_book = Book()
    iwm = []
    for day_i, day in enumerate(days):
        session = day["session"]
        opx, cpx = view.session_open_close("IWM", session)
        if opx is None or cpx is None:
            raise SystemExit(f"IWM missing {session}")
        result = advance_book(
            iwm_book,
            side="long",
            pick_names=["IWM"],
            open_of={"IWM": opx},
            close_of={"IWM": cpx},
            day_i=day_i,
            fees=fees,
            final_session=session == LAST_SESSION,
        )
        iwm.append(round(float(result["ret_f"]), 8))
    matrix = []
    for draw_i in range(RANDOM4_DRAWS):
        rng = random.Random(RANDOM4_SEED + draw_i)
        book = Book()
        row = []
        for day_i, day in enumerate(days):
            pool = sorted(day["names"])
            k = min(RANDOM4_N, len(pool))
            picks = rng.sample(pool, k) if k else []
            result = advance_book(
                book,
                side="long",
                pick_names=picks,
                open_of=day["open_of"],
                close_of=day["close_of"],
                day_i=day_i,
                fees=fees,
                final_session=day["session"] == LAST_SESSION,
            )
            row.append(round(float(result["ret_f"]), 8))
        matrix.append(row)
        if draw_i % 200 == 0:
            print(f"random4 {draw_i}", flush=True)
    return iwm, matrix


def _fmt_pct(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value) * 100:.2f}%"


def _fmt_num(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.2f}"


def _info_luck(rets: list[float], iwm: list[float], matrix: list[list[float]], indexes: list[int]) -> dict:
    strat = compound([rets[i] for i in indexes]) if indexes else 0.0
    iwm_c = compound([iwm[i] for i in indexes]) if indexes else 0.0
    draws = [compound([row[i] for i in indexes]) for row in matrix] if indexes else []
    beaten = sum(1 for value in draws if strat > value)
    return {
        "iwm": iwm_c,
        "luck_p": luck_p([rets[i] for i in indexes], LUCK_DENOMINATOR) if indexes else 1.0,
        "random4_beaten": (beaten / len(draws)) if draws else None,
        "random4_mean": (sum(draws) / len(draws)) if draws else None,
        "raw_p": raw_luck_p([rets[i] for i in indexes]) if indexes else 1.0,
        "strategy": strat,
    }


def _write_report(view: BarView, fees: dict) -> None:
    recorded = _load_recorded(SESSIONS)
    series = _series(recorded)
    fresh = freeze_payload(_series(_load_recorded(RANK_SESSIONS)))
    frozen = json.loads(FROZEN_PATH.read_text(encoding="utf-8"))
    if fresh["rank1"] != frozen.get("rank1") or fresh["rows"] != frozen.get("rows"):
        raise SystemExit("recomputed rank does not match the frozen file")
    # Rebuild day objects for baselines from the same inputs already used.
    # Prices for RANDOM4 come from a second feature build so the pool is the
    # day's universe, not a later held-name add. That build is deterministic.
    cache: dict[str, bytes] = {}
    _manifest, index = load_manifest()
    grouped: dict[str, list[dict]] = {day: [] for day in SESSIONS}
    for row in index.values():
        grouped[row["date"]].append(row)
    built = []
    for session in SESSIONS:
        built.append(build_day(view, grouped.get(session) or [], session, cache))
    iwm, matrix = _baseline_returns(view, fees, built)
    rules = list(iter_rules())
    id_to_index = {rid: index for index, rid, *_rest in rules}
    before = list(RANK_SESSIONS)
    after = list(TEST_SESSIONS)
    before_idx = [i for i, session in enumerate(SESSIONS) if session in set(before)]
    after_idx = [i for i, session in enumerate(SESSIONS) if session in set(after)]
    rows_out = []
    for row in frozen["rows"]:
        index = id_to_index[row["id"]]
        item = {
            "after": _slice(series, index, after),
            "before": _slice(series, index, before),
            "id": row["id"],
            "label": row["label"],
            "luck_after": _info_luck(series["ret_f"][index], iwm, matrix, after_idx),
            "luck_before": _info_luck(series["ret_f"][index], iwm, matrix, before_idx),
            "rank": row["rank"],
        }
        rows_out.append(item)
    summary = {
        "label": STUDY_LABEL,
        "luck_denominator": LUCK_DENOMINATOR,
        "n": N,
        "nothing_fitted_after": FIT_CUTOFF,
        "rank1": frozen["rank1"],
        "rank1_label": frozen["rank1_label"],
        "rows": rows_out,
        "study": STUDY,
        "untestable": frozen["untestable"],
    }
    text = _render(summary)
    report = RETURNS / "REPORT.md"
    summary_path = RETURNS / "summary.json"
    raw_summary = (json.dumps(summary, indent=2, sort_keys=True) + "\n").encode("utf-8")
    raw_report = text.encode("utf-8")
    for path, raw in ((report, raw_report), (summary_path, raw_summary)):
        if path.is_file() and path.read_bytes() != raw:
            raise SystemExit(f"{path.name} would change")
        if not path.is_file():
            path.write_bytes(raw)
    rank1 = rows_out[0] if rows_out else None
    if rank1:
        print("RANK1", rank1["id"], frozen["rank1_label"], flush=True)
        print("BEFORE", json.dumps(rank1["before"], sort_keys=True), flush=True)
        print("AFTER", json.dumps(rank1["after"], sort_keys=True), flush=True)


def _table(rows: list[dict], key: str) -> str:
    header = (
        "| rank | rule | compound | up | down | flat | days entered | entries | closed | win rate | ex-best | top-ticker share |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
    )
    lines = [header]
    for row in rows:
        stats = row[key]
        lines.append(
            "| {rank} | `{id}` | {compound} | {up} | {down} | {flat} | {days} | {entries} | {closed} | {win} | {ex} | {share} |\n".format(
                rank=row["rank"],
                id=row["id"],
                compound=_fmt_pct(stats["compound"]),
                up=stats["up"],
                down=stats["down"],
                flat=stats["flat"],
                days=stats["days_entered"],
                entries=stats["entries"],
                closed=stats["closed"],
                win=_fmt_pct(stats["win_rate"]),
                ex=_fmt_pct(stats["ex_best"]),
                share=_fmt_pct(stats["top_share"]),
            )
        )
    return "".join(lines)


def _render(summary: dict) -> str:
    rows = summary["rows"]
    rank1 = rows[0] if rows else None
    lines = [
        f"# {STUDY}",
        "",
        f"Label: `{STUDY_LABEL}`. Creation date 2026-09-26, so every session is `designed_after`.",
        "",
        f"Frozen rank 1: `{summary['rank1']}` ({summary['rank1_label']}).",
        f"Nothing fitted after {summary['nothing_fitted_after']}. Rank used sessions through {RANK_END} only.",
        f"Luck denominator {summary['luck_denominator']} is information only. It did not choose the rank.",
        "",
    ]
    if rank1:
        lines.append("## Rank 1")
        lines.append("")
        for label, stats, luck in (
            ("before 2026-09-14", rank1["before"], rank1["luck_before"]),
            ("after 2026-09-14", rank1["after"], rank1["luck_after"]),
        ):
            lines.append(
                f"- {label}: compound {_fmt_pct(stats['compound'])} "
                f"(15bp {_fmt_pct(stats['compound_15'])}), "
                f"ex-best {_fmt_pct(stats['ex_best'])}, "
                f"top share {_fmt_pct(stats['top_share'])} ({stats['best']}), "
                f"closed {stats['closed']}, win {_fmt_pct(stats['win_rate'])}, "
                f"start-day win {_fmt_pct(stats['start_day_win'])}, "
                f"asymmetric {_fmt_num(stats['asymmetric'])}. "
                f"IWM {_fmt_pct(luck['iwm'])}, RANDOM4 mean {_fmt_pct(luck['random4_mean'])}, "
                f"beat { _fmt_pct(luck['random4_beaten']) } of draws. "
                f"Raw p {luck['raw_p']:.4f}, adjusted p {luck['luck_p']:.4f}."
            )
        lines.append("")
        keep = (
            rank1["before"]["closed"] >= MIN_CLOSED
            and rank1["before"]["win_rate"] is not None
            and rank1["before"]["win_rate"] > WIN_MIN
        )
        lines.append(
            f"Keep bar on the rank window (≥{MIN_CLOSED} closed trades and win rate > {WIN_MIN:.0%}): "
            + ("met" if keep else "not met")
            + "."
        )
        lines.append("")
    lines.append("## Before 2026-09-14")
    lines.append("")
    lines.append(_table(rows, "before"))
    lines.append("")
    lines.append("## After 2026-09-14")
    lines.append("")
    lines.append(_table(rows, "after"))
    lines.append("")
    lines.append("## Top 10")
    lines.append("")
    lines.append(_table(rows[:10], "before"))
    lines.append("")
    lines.append(_table(rows[:10], "after"))
    lines.append("")
    if summary["untestable"]:
        lines.append("## Untestable")
        lines.append("")
        for row in summary["untestable"]:
            lines.append(f"- `{row['id']}`")
        lines.append("")
    lines.append("Luck, IWM, and RANDOM4 are information. They are not the rank.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("train", "test"), required=True)
    args = parser.parse_args()
    walk_phase(args.phase)


if __name__ == "__main__":
    main()
