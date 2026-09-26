"""Research-only forward book for frozen Factor Mine recipes.

Each sleeve starts on 2026-09-28 with its own $10,000. Inputs are the
earliest committed ``data/factor_mine/send_inputs/<date>.json``. The
fill model is keep-held: a name that is still held and still selected
is not sold and not bought, and it pays no fee. This module does not
place orders.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.factor_mine import pick_day, should_exit
from src.factor_mine_book import lot_should_sell
from src.factor_mine_send_inputs import rows_for_record
from src.lever_search_proof import build_group3_recipes
from src.paper_trade import order_fees

ROOT = Path(__file__).resolve().parents[1]
STUDY_NAME = "forward_shadow_v1"
STUDY_DIR = ROOT / "research" / STUDY_NAME
FORWARD_START = "2026-09-28"
CAPITAL = 10_000.0
FILL_MODEL = "keep-held"
SELL_MODE = "list"
DAY_CAP = 1.0
HARD_RED_MAX = -3.0
FEES_PATH = ROOT / "00_grounding" / "futubull_fees.json"
FEES_SHA256 = "019ebdba0fc0b20e02c91f116dc5591b81e96630f60c110dfbfd3b5d8e16c0d3"
BP_SIDE = 0.000075
RANDOM_SEED = 20260813
RANDOM_DRAWS = 1000
SEND_DIR = "data/factor_mine/send_inputs"
PAIRS = (
    ("fwd_union_hot_n4_h1", "union_hot_n4_h1"),
    ("fwd_union_hot_score_h3", "union_hot_score_h3"),
)
# PR #370 lock. protocol.py and PREREG.md are unchanged from this commit
# through the PR head 0f323096993747fc55a4d821c9158fab4ad9f920.
V4_COMMIT = "7da63e4f6cf4214b072d462cec04feddaa60147f"
V4_SPEC_SHA256 = "0503a5a549a66093754c9102eb430d01bc035687b08bca4f04d699e3a7daeca7"
HOLDUP_S = 0.0
HOLDUP_SESS = 2
_V4_ALARM = {"alarm": True}
_V4_NONEWS = {"alarm": True, "news": "bad"}


class ForwardError(Exception):
    """A write would change an earlier session or break the frozen spec."""


def canon(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def study_dir(root: Path | None = None) -> Path:
    return root or STUDY_DIR


def ledger_dir(root: Path | None = None) -> Path:
    return study_dir(root) / "ledger"


def bars_dir(root: Path | None = None) -> Path:
    return study_dir(root) / "bars"


def spec_path(root: Path | None = None) -> Path:
    return study_dir(root) / "recipes.json"


def hook_path(root: Path | None = None) -> Path:
    return study_dir(root) / "hooks" / "v4_winners.json"


def picks_path(root: Path | None, session: str) -> Path:
    return ledger_dir(root) / f"{session}.picks.json"


def fills_path(root: Path | None, session: str) -> Path:
    return ledger_dir(root) / f"{session}.fills.json"


def bar_path(root: Path | None, session: str) -> Path:
    return bars_dir(root) / f"{session}.json"


def manifest_path(root: Path | None = None) -> Path:
    return ledger_dir(root) / "manifest.jsonl"


def report_path(root: Path | None = None) -> Path:
    return ledger_dir(root) / "REPORT.md"


def fee_15(shares: int, price: float) -> float:
    """Flat 7.5 bp of notional. One side of the 15 bp round trip."""
    if shares <= 0 or price <= 0:
        return 0.0
    return round(shares * price * BP_SIDE, 4)


def load_fees() -> dict:
    raw = FEES_PATH.read_bytes()
    digest = sha256_bytes(raw)
    if digest != FEES_SHA256:
        raise ForwardError("Futubull fee file does not match the pinned sha256")
    return json.loads(raw.decode("utf-8"))


def _git(args: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=False, capture_output=True,
    )


def send_path(session: str) -> str:
    return f"{SEND_DIR}/{session}.json"


def earliest_send_inputs(session: str, *, rev: str = "origin/main",
                         cwd: Path | None = None) -> dict | None:
    """Earliest committed send file for this session. Later edits are not read."""
    cwd = cwd or ROOT
    path = send_path(session)
    revs = [rev, "HEAD"]
    commit = ""
    for candidate in revs:
        proc = _git(
            ["log", candidate, "--diff-filter=A", "--reverse", "--pretty=%H", "--", path],
            cwd,
        )
        if proc.returncode != 0:
            continue
        lines = [line.strip() for line in proc.stdout.decode("utf-8", "replace").splitlines() if line.strip()]
        if lines:
            commit = lines[0]
            break
    if not commit:
        return None
    blob = _git(["rev-parse", f"{commit}:{path}"], cwd)
    if blob.returncode != 0:
        raise ForwardError(f"send_inputs blob missing at {commit}")
    blob_sha = blob.stdout.decode("utf-8").strip()
    raw = _git(["cat-file", "-p", blob_sha], cwd)
    if raw.returncode != 0:
        raise ForwardError(f"send_inputs blob unreadable {blob_sha}")
    try:
        doc = json.loads(raw.stdout.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ForwardError("send_inputs blob is not json") from exc
    if not isinstance(doc, dict):
        raise ForwardError("send_inputs blob is not a document")
    return {
        "doc": doc,
        "commit": commit,
        "blob_sha": blob_sha,
        "path": path,
        "raw": raw.stdout,
    }


def send_score(doc: dict | None) -> float | None:
    """Morning S only when the frozen send file itself carries it."""
    if not doc:
        return None
    for key in ("s", "predict_score", "score"):
        if doc.get(key) is None:
            continue
        try:
            return float(doc[key])
        except (TypeError, ValueError):
            continue
    return None


def group3_by_name() -> dict[str, dict]:
    return {rec["name"]: rec for rec in build_group3_recipes()}


def frozen_recipes() -> list[dict]:
    """Group 3 bodies under the forward names. Gates are copied, not edited."""
    found = group3_by_name()
    out = []
    for new, old in PAIRS:
        if old not in found:
            raise ForwardError(f"group3 recipe missing: {old}")
        body = {key: found[old][key] for key in found[old]}
        body["name"] = new
        body["source_name"] = old
        out.append(body)
    return out


def _v4_part(name, *, rank="hot_score", top_n=8, hold=3, sell="list",
             s_boost="none", require=None, forbid=None, exit_when=None,
             earn_news=False, skip_first=False, base=False, already_picked=False):
    """Same part row as factor_mine_recipe_search_v4.protocol._part."""
    return {
        "already_picked": already_picked,
        "base": base,
        "earn_news": earn_news,
        "exit_when": dict(exit_when or {}),
        "forbid": dict(forbid or _V4_ALARM),
        "hold": hold,
        "name": name,
        "rank": rank,
        "require": dict(require or {}),
        "s_boost": s_boost,
        "sell": sell,
        "side": "long",
        "skip_first": skip_first,
        "top_n": top_n,
        "universe": "union",
    }


def _with_keep_held_fingerprint(row: dict) -> dict:
    """Fingerprint covers the frozen candidate plus fill_model keep-held."""
    body = dict(row)
    body["fill_model"] = FILL_MODEL
    body["v4_commit"] = V4_COMMIT
    body["v4_spec_sha256"] = V4_SPEC_SHA256
    body["fingerprint_sha256"] = sha256_text(canon(body))
    return body


def v4_carry_recipes() -> list[dict]:
    """Three not-rejected weather-off carry-forwards from PR #370.

    Bodies are the locked ``candidates()`` rows: the part, ``weather``
    false, and ``id`` ``name__w0``. ``already_picked`` on
    ``union_hot_n4_h1`` is the frozen label, not a gate.
    """
    parts = (
        _v4_part("union_hot_n4_h1", top_n=4, hold=1, already_picked=True),
        _v4_part("union_hot_n4_holdup", top_n=4, hold=1, s_boost="holdup"),
        _v4_part("union_hot_n4_h1_nonews", top_n=4, hold=1, forbid=_V4_NONEWS),
    )
    out = []
    for part in parts:
        row = dict(part)
        row["weather"] = False
        row["id"] = f"{part['name']}__w0"
        out.append(_with_keep_held_fingerprint(row))
    return out


def recipe_key(recipe: dict) -> str:
    """Sleeve name. v4 rows are keyed by the frozen candidate id."""
    ident = recipe.get("id")
    if ident:
        return str(ident)
    return str(recipe["name"])


def spec_body() -> dict:
    return {
        "baselines": {
            "IWM": "buy at the first locked session open, whole shares, Futubull fee, mark at each close",
            "RANDOM4": (
                "1000 draws, random.Random(20260813 + draw), 4 names from that "
                "day's frozen universe, long, hold 1, time exit at the next "
                "session open. A baseline, not the keep-held book."
            ),
        },
        "capital": CAPITAL,
        "day_cap": DAY_CAP,
        "fees_sha256": FEES_SHA256,
        "fill_model": FILL_MODEL,
        "fill_px": "09:30 open",
        "flat_15bp_side": BP_SIDE,
        "forward_start": FORWARD_START,
        "hard_red_max": HARD_RED_MAX,
        "hard_red_rule": (
            "When the frozen send file carries s and s is at or below "
            "hard_red_max, new buys sit. A recipe with weather false does not sit. "
            "A missing s does not sit. "
            "List-drop sells still happen. A kept lot is not liquidated."
        ),
        "inputs": "earliest committed data/factor_mine/send_inputs/<date>.json",
        "mark_px": "session close",
        "min_hold": (
            "recipe hold in locked sessions; the entry session counts as zero. "
            "When s_boost is holdup, s is present, and s is above 0, "
            "the new lot's min-hold is 2."
        ),
        "random4_draws": RANDOM_DRAWS,
        "random4_seed": RANDOM_SEED,
        "recipes": frozen_recipes() + v4_carry_recipes(),
        "renewal": (
            "A held name that is still selected that morning stays. "
            "The book records no sell and no buy for that name and charges no fee."
        ),
        "sell": SELL_MODE,
        "shares": "whole",
        "size": "leftover",
        "study": STUDY_NAME,
        "v4_carry": {
            "commit": V4_COMMIT,
            "ids": [recipe["id"] for recipe in v4_carry_recipes()],
            "spec_sha256": V4_SPEC_SHA256,
            "weather": False,
        },
        "v4_hook": (
            "hooks/v4_winners.json may only gain winners. first_session is "
            "the first locked session after the fingerprint commit. Earlier "
            "sessions stay as written."
        ),
    }


def build_spec_document() -> dict:
    body = spec_body()
    doc = dict(body)
    doc["spec_sha256"] = sha256_text(canon(body))
    return doc


def load_spec(root: Path | None = None) -> dict:
    path = spec_path(root)
    doc = json.loads(path.read_text(encoding="utf-8"))
    body = {key: value for key, value in doc.items() if key != "spec_sha256"}
    digest = sha256_text(canon(body))
    if digest != doc.get("spec_sha256"):
        raise ForwardError("recipe spec sha256 does not match the file")
    if doc.get("fill_model") != FILL_MODEL:
        raise ForwardError("fill model is not keep-held")
    return doc


def assert_spec_matches_group3(root: Path | None = None) -> dict:
    """Group 3 copies stay exact. The three v4 carry-forwards stay pinned."""
    disk = load_spec(root)
    fresh = build_spec_document()
    if disk != fresh:
        raise ForwardError("committed recipe spec drifted from the frozen recipes")
    found = group3_by_name()
    expected = dict(PAIRS)
    group3 = [rec for rec in disk["recipes"] if rec["name"] in expected]
    if [rec["name"] for rec in group3] != [name for name, _old in PAIRS]:
        raise ForwardError("group3 recipe order drift")
    for rec in group3:
        if expected.get(rec["name"]) != rec["source_name"]:
            raise ForwardError("recipe rename drift")
        source = found[rec["source_name"]]
        for key, value in source.items():
            if key == "name":
                continue
            if rec.get(key) != value:
                raise ForwardError(f"recipe gate drift {rec['name']} {key}")
        extra = set(rec) - set(source) - {"source_name"}
        if extra:
            raise ForwardError(f"group3 recipe gained fields {rec['name']} {sorted(extra)}")
    carry = [rec for rec in disk["recipes"] if rec.get("id")]
    pinned = v4_carry_recipes()
    if [rec["id"] for rec in carry] != [rec["id"] for rec in pinned]:
        raise ForwardError("v4 carry-forward drift")
    if [recipe_key(rec) for rec in disk["recipes"]] != [
        recipe_key(rec) for rec in frozen_recipes()
    ] + [rec["id"] for rec in pinned]:
        raise ForwardError("recipe order drift")
    for rec, row in zip(carry, pinned):
        if rec != row:
            raise ForwardError(f"v4 recipe body drift {rec.get('id')}")
        if rec.get("fill_model") != FILL_MODEL or rec.get("weather") is not False:
            raise ForwardError(f"v4 recipe is not keep-held weather-off {rec.get('id')}")
        covered = {key: value for key, value in rec.items() if key != "fingerprint_sha256"}
        if sha256_text(canon(covered)) != rec.get("fingerprint_sha256"):
            raise ForwardError(f"v4 recipe fingerprint drift {rec.get('id')}")
        if covered.get("v4_commit") != V4_COMMIT or covered.get("v4_spec_sha256") != V4_SPEC_SHA256:
            raise ForwardError(f"v4 citation drift {rec.get('id')}")
    return disk


def initial_state() -> dict:
    return {
        "cash_15": CAPITAL,
        "cash_f": CAPITAL,
        "equity_15": CAPITAL,
        "equity_f": CAPITAL,
        "positions": [],
    }


def _positions(state: dict) -> dict[str, dict]:
    return {str(lot["ticker"]): dict(lot) for lot in (state.get("positions") or [])}


def _state_from(cash_f: float, cash_15: float, pos: dict[str, dict],
                equity_f: float, equity_15: float) -> dict:
    rows = [pos[ticker] for ticker in sorted(pos)]
    return {
        "cash_15": round(float(cash_15), 4),
        "cash_f": round(float(cash_f), 4),
        "equity_15": round(float(equity_15), 4),
        "equity_f": round(float(equity_f), 4),
        "positions": rows,
    }


def _sits_new_buys(recipe: dict, s: float | None) -> bool:
    """Weather off buys on a hard-red morning. A missing weather gate sits."""
    if "weather" in recipe and not recipe["weather"]:
        return False
    return s is not None and float(s) <= float(HARD_RED_MAX)


def _entry_min_hold(recipe: dict, s: float | None, sit: bool) -> int:
    """Holdup sets a new lot's min-hold to 2 when S is present and above 0."""
    base = int(recipe["hold"])
    boost = recipe.get("s_boost") or "none"
    holdup = boost == "holdup" and s is not None and float(s) > HOLDUP_S and not sit
    if holdup:
        return max(base, HOLDUP_SESS)
    return base


def step_day(recipe: dict, state: dict, session: str, sessions: list[str],
             selected: list[str], rows_by_ticker: dict[str, dict],
             opens: dict[str, float], closes: dict[str, float], fees: dict,
             *, s: float | None) -> tuple[dict, dict]:
    """One session of one sleeve. Renewals are not trades."""
    if session not in sessions:
        raise ForwardError("session missing from the locked calendar")
    side = recipe.get("side") or "long"
    min_hold = int(recipe["hold"])
    sell_mode = recipe["sell"] if "sell" in recipe else SELL_MODE
    pos = _positions(state)
    index = {day: i for i, day in enumerate(sessions)}
    here = index[session]
    selected_set = set(selected)
    hard_red = _sits_new_buys(recipe, s)
    entry_min = _entry_min_hold(recipe, s, hard_red)
    sells: list[dict] = []
    buys: list[dict] = []
    renewals: list[str] = []
    skips: list[dict] = []
    closed: list[dict] = []
    ticker_pnl: dict[str, float] = {}
    cash_f = float(state["cash_f"])
    cash_15 = float(state["cash_15"])

    for ticker in sorted(pos):
        lot = pos[ticker]
        held = here - index[str(lot["entry"])]
        row = rows_by_ticker.get(ticker) or {}
        early = bool(should_exit(row, recipe.get("exit_when")))
        dropped = ticker not in selected_set
        px = opens.get(ticker)
        if px is None or px <= 0:
            skips.append({
                "kind": "no_price", "ticker": ticker,
                "reason": "no 09:30 open — carry at last price",
            })
            continue
        px = float(px)
        entry_px = float(lot["entry_px"])
        if side == "long":
            lot["peak_px"] = round(max(float(lot.get("peak_px") or entry_px), px), 6)
        else:
            lot["peak_px"] = round(min(float(lot.get("peak_px") or entry_px), px), 6)
        lot_min = int(lot.get("min_hold") or min_hold)
        do_sell, kind = lot_should_sell(
            lot, held=held, min_hold=lot_min,
            early=early, dropped=dropped, sell_mode=sell_mode, px=px, side=side,
            take_pct=None, stop_pct=None,
        )
        if not do_sell:
            if ticker in selected_set:
                renewals.append(ticker)
            elif dropped:
                skips.append({
                    "kind": "min_hold", "ticker": ticker,
                    "reason": f"dropped but min-hold {held}/{lot_min}",
                })
            continue
        shares = int(lot["shares"])
        fee_side = "sell" if side == "long" else "buy"
        fee_f = order_fees(shares, px, fee_side, fees)
        fee_b = fee_15(shares, px)
        prev = float(lot.get("prev_mark", entry_px))
        if side == "long":
            cash_f += shares * px - fee_f
            cash_15 += shares * px - fee_b
            day_pnl = shares * (px - prev) - fee_f
            round_pnl = (
                shares * (px - entry_px) - fee_f - float(lot.get("fee_in_f") or 0)
                + float(lot.get("realized") or 0.0)
            )
        else:
            cash_f -= shares * px + fee_f
            cash_15 -= shares * px + fee_b
            day_pnl = shares * (prev - px) - fee_f
            round_pnl = (
                shares * (entry_px - px) - fee_f - float(lot.get("fee_in_f") or 0)
                + float(lot.get("realized") or 0.0)
            )
        ticker_pnl[ticker] = ticker_pnl.get(ticker, 0.0) + day_pnl
        pos.pop(ticker)
        trade_side = "SELL" if side == "long" else "COVER"
        sells.append({
            "fees": fee_f,
            "fees_15": fee_b,
            "kind": kind,
            "price": round(px, 6),
            "shares": shares,
            "side": trade_side,
            "ticker": ticker,
        })
        closed.append({
            "entry": lot["entry"],
            "entry_px": round(entry_px, 6),
            "exit": session,
            "exit_px": round(px, 6),
            "pnl": round(round_pnl, 4),
            "shares": shares,
            "ticker": ticker,
            "win": round_pnl > 0,
        })

    cap = recipe.get("weight_cap")
    if side == "long" and isinstance(cap, (int, float)) and not isinstance(cap, bool) and 0.0 < float(cap) < 1.0:
        from research.concentration_cap_v1.engine import shares_to_keep

        trimmed: list[str] = []
        for ticker in sorted(pos):
            lot = pos.get(ticker)
            if lot is None:
                continue
            px = opens.get(ticker)
            if px is None or float(px) <= 0:
                continue
            px = float(px)
            equity = cash_f + _open_stock(pos, opens, side)
            shares = int(lot["shares"])
            keep = shares_to_keep(shares, px, equity, float(cap), fees)
            if keep >= shares:
                continue
            sold_n = shares - keep
            fee_f = order_fees(sold_n, px, "sell", fees)
            fee_b = fee_15(sold_n, px)
            prev = float(lot.get("prev_mark", lot["entry_px"]))
            entry_px = float(lot["entry_px"])
            cash_f += sold_n * px - fee_f
            cash_15 += sold_n * px - fee_b
            day_pnl = sold_n * (px - prev) - fee_f
            ticker_pnl[ticker] = ticker_pnl.get(ticker, 0.0) + day_pnl
            fee_in = float(lot.get("fee_in_f") or 0.0)
            fee_in_15 = float(lot.get("fee_in_15") or 0.0)
            chunk = sold_n * (px - entry_px) - fee_f - fee_in * (sold_n / shares)
            sells.append({
                "fees": fee_f,
                "fees_15": fee_b,
                "kind": "trim",
                "price": round(px, 6),
                "shares": sold_n,
                "side": "SELL",
                "ticker": ticker,
            })
            if keep < 1:
                round_pnl = chunk + float(lot.get("realized") or 0.0)
                closed.append({
                    "entry": lot["entry"],
                    "entry_px": round(entry_px, 6),
                    "exit": session,
                    "exit_px": round(px, 6),
                    "pnl": round(round_pnl, 4),
                    "shares": shares,
                    "ticker": ticker,
                    "win": round_pnl > 0,
                })
                pos.pop(ticker)
            else:
                lot["fee_in_15"] = fee_in_15 * (keep / shares)
                lot["fee_in_f"] = fee_in * (keep / shares)
                lot["realized"] = float(lot.get("realized") or 0.0) + chunk
                lot["shares"] = keep
            trimmed.append(ticker)
        if trimmed:
            renewals[:] = [ticker for ticker in renewals if ticker not in trimmed]

    new = [ticker for ticker in selected if ticker not in pos]
    if hard_red:
        for ticker in new:
            skips.append({
                "kind": "hard_red", "ticker": ticker,
                "reason": "hard-red sit; no new buy",
            })
        new = []
    if new and (cash_f > 0 or side == "short"):
        room = cash_f * DAY_CAP if side == "long" else max(0.0, (cash_f + _open_stock(pos, opens, side)) * 0.5)
        room = max(0.0, room)
        budgets = [room / len(new)] * len(new)
        for ticker, per in zip(new, budgets):
            px = opens.get(ticker)
            if px is None or px <= 0:
                skips.append({"kind": "no_price", "ticker": ticker, "reason": "no 09:30 open"})
                continue
            px = float(px)
            shares = int(per // px)
            if shares < 1:
                skips.append({
                    "kind": "cash", "ticker": ticker,
                    "reason": f"leftover split {per:.2f} < 1 share",
                })
                continue
            fee_side = "buy" if side == "long" else "sell"
            fee_f = order_fees(shares, px, fee_side, fees)
            if side == "long":
                cost = shares * px + fee_f
                if cost > cash_f + 1e-6:
                    shares = int((cash_f - fee_f) // px) if px else 0
                    if shares < 1:
                        skips.append({
                            "kind": "cash", "ticker": ticker,
                            "reason": "cash cannot buy 1 share",
                        })
                        continue
                    fee_f = order_fees(shares, px, "buy", fees)
                    cost = shares * px + fee_f
                fee_b = fee_15(shares, px)
                cash_f -= cost
                cash_15 -= shares * px + fee_b
            else:
                fee_f = order_fees(shares, px, "sell", fees)
                fee_b = fee_15(shares, px)
                cash_f += shares * px - fee_f
                cash_15 += shares * px - fee_b
            pos[ticker] = {
                "entry": session,
                "entry_px": round(px, 6),
                "fee_in_15": fee_b,
                "fee_in_f": fee_f,
                "last_px": round(px, 6),
                "min_hold": entry_min,
                "peak_px": round(px, 6),
                "prev_mark": round(px, 6),
                "shares": shares,
                "ticker": ticker,
            }
            buys.append({
                "fees": fee_f,
                "fees_15": fee_b,
                "price": round(px, 6),
                "shares": shares,
                "side": "BUY" if side == "long" else "SHORT",
                "ticker": ticker,
            })

    for ticker, lot in pos.items():
        mark = closes.get(ticker)
        if mark is None or mark <= 0:
            mark = float(lot.get("last_px") or lot["entry_px"])
        mark = float(mark)
        prev = float(lot.get("prev_mark", lot["entry_px"]))
        if side == "long":
            mtm = lot["shares"] * (mark - prev)
        else:
            mtm = lot["shares"] * (prev - mark)
        if lot["entry"] == session:
            mtm -= float(lot.get("fee_in_f") or 0)
        ticker_pnl[ticker] = ticker_pnl.get(ticker, 0.0) + mtm
        lot["last_px"] = round(mark, 6)
        lot["prev_mark"] = round(mark, 6)

    equity_f, equity_15 = _equity(cash_f, cash_15, pos, side)
    start_f = float(state.get("equity_f") or CAPITAL)
    start_15 = float(state.get("equity_15") or CAPITAL)
    ret_f = (equity_f / start_f - 1.0) if start_f else 0.0
    ret_15 = (equity_15 / start_15 - 1.0) if start_15 else 0.0
    fees_f = round(sum(row["fees"] for row in sells + buys), 4)
    fees_b = round(sum(row["fees_15"] for row in sells + buys), 4)
    day = {
        "buys": buys,
        "closed_trades": closed,
        "equity_flat_15bp": equity_15,
        "equity_futubull": equity_f,
        "fees_flat_15bp": fees_b,
        "fees_futubull": fees_f,
        "hard_red": hard_red,
        "renewals": renewals,
        "ret_flat_15bp": round(ret_15, 8),
        "ret_futubull": round(ret_f, 8),
        "sells": sells,
        "skips": skips,
        "ticker_pnl": {key: round(value, 4) for key, value in sorted(ticker_pnl.items())},
    }
    return _state_from(cash_f, cash_15, pos, equity_f, equity_15), day


def _open_stock(pos: dict[str, dict], opens: dict[str, float], side: str) -> float:
    total = 0.0
    for lot in pos.values():
        px = opens.get(lot["ticker"])
        if px is None or px <= 0:
            px = lot.get("last_px")
        if px is None:
            continue
        notion = lot["shares"] * float(px)
        total += notion if side == "long" else -notion
    return total


def _equity(cash_f: float, cash_15: float, pos: dict[str, dict], side: str) -> tuple[float, float]:
    stock = 0.0
    for lot in pos.values():
        notion = lot["shares"] * float(lot["last_px"])
        stock += notion if side == "long" else -notion
    return round(cash_f + stock, 4), round(cash_15 + stock, 4)


def _check_session(session: str) -> str:
    day = str(session)[:10]
    if day < FORWARD_START:
        raise ForwardError("no day before 2026-09-28 is filled")
    return day


def _read_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canon(doc) + "\n", encoding="utf-8")


def manifest_lines(root: Path | None = None) -> list[str]:
    path = manifest_path(root)
    if not path.is_file():
        return []
    text = path.read_text(encoding="utf-8")
    if text == "":
        return []
    return text.splitlines()


def _append_manifest(root: Path | None, row: dict) -> None:
    line = canon(row)
    lines = manifest_lines(root)
    if line in lines:
        return
    path = manifest_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = ("".join(item + "\n" for item in lines)) + line + "\n"
    path.write_text(payload, encoding="utf-8")


def _sessions_of(kind: str, root: Path | None) -> list[str]:
    out = []
    for line in manifest_lines(root):
        row = json.loads(line)
        if row.get("kind") == kind:
            out.append(str(row["session"]))
    return out


def load_winners(root: Path | None = None) -> list[dict]:
    path = hook_path(root)
    if not path.is_file():
        return []
    doc = json.loads(path.read_text(encoding="utf-8"))
    winners = doc.get("winners") or []
    if not isinstance(winners, list):
        raise ForwardError("v4 hook winners must be a list")
    return winners


def recipe_fingerprint(recipe: dict) -> str:
    return sha256_text(canon(recipe))


def validate_winner(winner: dict, *, last_session: str | None,
                    commit_day: str | None = None) -> None:
    name = str(winner.get("name") or "")
    if not name.startswith("fwd_") or name in dict(PAIRS):
        raise ForwardError("v4 winner needs a new fwd_ name")
    recipe = winner.get("recipe")
    if not isinstance(recipe, dict) or recipe.get("name") != name:
        raise ForwardError("v4 winner recipe name mismatch")
    if recipe_fingerprint(recipe) != winner.get("fingerprint_sha256"):
        raise ForwardError("v4 winner fingerprint mismatch")
    first = str(winner.get("first_session") or "")
    if first < FORWARD_START:
        raise ForwardError("v4 winner cannot start before 2026-09-28")
    if last_session and first <= last_session:
        raise ForwardError("v4 winner would backfill a locked session")
    if commit_day and first <= commit_day:
        raise ForwardError("v4 first session is not after the fingerprint commit")


def active_recipes(session: str, root: Path | None, spec: dict | None = None) -> list[dict]:
    spec = spec or load_spec(root)
    recipes = list(spec["recipes"])
    known = {recipe_key(rec) for rec in recipes}
    for winner in load_winners(root):
        if str(winner.get("first_session") or "") > session:
            continue
        validate_winner(winner, last_session=None)
        if recipe_key(winner["recipe"]) in known or winner["name"] in known:
            raise ForwardError("duplicate recipe name")
        prior = [
            day for day in _sessions_of("picks", root)
            if winner["first_session"] <= day < session
        ]
        for day in prior:
            picks = _read_json(picks_path(root, day)) or {}
            if winner["name"] not in (picks.get("recipes") or {}):
                raise ForwardError(f"{winner['name']} would backfill {day}")
        recipes.append(winner["recipe"])
        known.add(recipe_key(winner["recipe"]))
    return recipes


def _pool(rows: list[dict], recipe: dict) -> list[dict]:
    """skip_first and earn_news, as the v4 book applies them before pick_day."""
    pool = rows
    if recipe.get("skip_first"):
        pool = [row for row in pool if int(row.get("days_on_list") or 1) > 1]
    if recipe.get("earn_news"):
        pool = [
            row for row in pool
            if row.get("erd_earn_react") or (row.get("boxes") or {}).get("news") == "good"
        ]
    return pool


def _selected(rows: list[dict], recipe: dict) -> tuple[list[str], dict[str, dict]]:
    chosen = pick_day(_pool(rows, recipe), recipe)
    names = []
    by_ticker = {}
    for row in rows:
        ticker = str(row.get("ticker") or "").upper()
        if ticker:
            by_ticker.setdefault(ticker, row)
    for row in chosen:
        ticker = str(row.get("ticker") or "").upper()
        if ticker and ticker not in names:
            names.append(ticker)
    return names, by_ticker


def build_picks(session: str, loaded: dict | None, root: Path | None,
                spec: dict | None = None) -> dict:
    spec = spec or load_spec(root)
    recipes = active_recipes(session, root, spec)
    sat = loaded is None or not rows_for_record(session, loaded["doc"] if loaded else None)
    reason = ""
    rows: list[dict] = []
    score = None
    pin = None
    if loaded is None:
        reason = "no frozen send_inputs"
    else:
        score = send_score(loaded["doc"])
        rows = rows_for_record(session, loaded["doc"])
        if str(loaded["doc"].get("source") or "") != "panel" or not rows:
            sat = True
            reason = "frozen send set has no same-day rows"
        pin = {
            "blob_sha": loaded["blob_sha"],
            "commit": loaded["commit"],
            "path": loaded["path"],
        }
    out_recipes = {}
    universe = []
    if not sat:
        seen = []
        for row in rows:
            ticker = str(row.get("ticker") or "").upper()
            if ticker and ticker not in seen:
                seen.append(ticker)
        universe = sorted(seen)
    for recipe in recipes:
        names, _by = _selected(rows, recipe) if not sat else ([], {})
        out_recipes[recipe_key(recipe)] = {
            "picks": names,
            "source_name": recipe.get("source_name") or "",
        }
    return {
        "fill_model": FILL_MODEL,
        "kind": "picks",
        "reason": reason,
        "recipes": out_recipes,
        "s": score,
        "sat_out": sat,
        "send_inputs": pin,
        "session": session,
        "spec_sha256": spec["spec_sha256"],
        "study": STUDY_NAME,
        "universe": universe,
    }


def record_picks(session: str, *, root: Path | None = None,
                 loaded: dict | None = None, allow_missing: bool = False,
                 spec: dict | None = None) -> str:
    """Lock the morning list. A missing send file waits; it does not sit early."""
    session = _check_session(session)
    if spec is None:
        spec = assert_spec_matches_group3(None) if root is None else load_spec(root)
    prior = _sessions_of("picks", root)
    if prior and session <= prior[-1] and session not in prior:
        raise ForwardError("session not after the last recorded one")
    path = picks_path(root, session)
    if loaded is None and not allow_missing:
        return "waiting"
    doc = build_picks(session, loaded, root, spec)
    text = canon(doc) + "\n"
    if path.is_file():
        if path.read_bytes() != text.encode("utf-8"):
            raise ForwardError(f"existing picks changed ({session})")
        _append_manifest(root, _manifest_row("picks", session, path, spec["spec_sha256"]))
        return "unchanged"
    if prior and session <= prior[-1]:
        raise ForwardError("session not after the last recorded one")
    _write_json(path, doc)
    _append_manifest(root, _manifest_row("picks", session, path, spec["spec_sha256"]))
    write_report(root)
    return "wrote"


def _manifest_row(kind: str, session: str, path: Path, spec_sha: str,
                  extra: dict | None = None) -> dict:
    raw = path.read_bytes()
    row = {
        "file": f"ledger/{path.name}",
        "fill_model": FILL_MODEL,
        "kind": kind,
        "session": session,
        "sha256": sha256_bytes(raw),
        "spec_sha256": spec_sha,
    }
    if extra:
        row.update(extra)
    return row


def _bars_map(doc: dict) -> tuple[dict[str, float], dict[str, float]]:
    opens = {}
    closes = {}
    for row in doc.get("bars") or []:
        ticker = str(row.get("ticker") or "").upper()
        if not ticker:
            continue
        if row.get("open") is not None and float(row["open"]) > 0:
            opens[ticker] = float(row["open"])
        if row.get("close") is not None and float(row["close"]) > 0:
            closes[ticker] = float(row["close"])
    return opens, closes


def _prior_state(root: Path | None, session: str, name: str) -> tuple[dict, list[str]]:
    sessions = []
    state = initial_state()
    for day in _sessions_of("fills", root):
        if day >= session:
            break
        doc = _read_json(fills_path(root, day)) or {}
        sleeve = (doc.get("recipes") or {}).get(name)
        if sleeve is None:
            continue
        sessions.append(day)
        state = sleeve["state"]
    sessions.append(session)
    return state, sessions


def build_fills(session: str, picks: dict, bars: dict, fees: dict,
                root: Path | None, spec: dict,
                history: list[dict] | None = None) -> dict:
    from src.forward_shadow_v1_bars import assert_split_consistent

    assert_split_consistent(list(history or []) + [bars])
    opens, closes = _bars_map(bars)
    if "IWM" not in opens or "IWM" not in closes:
        raise ForwardError("IWM bar missing")
    recipes = {recipe_key(rec): rec for rec in active_recipes(session, root, spec)}
    if set(recipes) != set((picks.get("recipes") or {})):
        raise ForwardError("picks recipe set does not match the active book")
    rows = []
    by_ticker: dict[str, dict] = {}
    loaded_rows = picks.get("_rows") or []
    for row in loaded_rows:
        item = dict(row)
        ticker = str(item.get("ticker") or "").upper()
        if not ticker:
            continue
        item["ticker"] = ticker
        rows.append(item)
        by_ticker.setdefault(ticker, item)
    score = picks.get("s")
    out = {}
    for name, recipe in recipes.items():
        listed = list((picks["recipes"][name].get("picks") or []))
        if rows:
            fresh, by_ticker = _selected(rows, recipe)
            if fresh != listed:
                raise ForwardError(f"picks drifted for {name}")
        state, sessions = _prior_state(root, session, name)
        state, day = step_day(
            recipe, state, session, sessions, listed, by_ticker,
            opens, closes, fees, s=score,
        )
        out[name] = {
            "buys": day["buys"],
            "closed_trades": day["closed_trades"],
            "equity_flat_15bp": day["equity_flat_15bp"],
            "equity_futubull": day["equity_futubull"],
            "fees_flat_15bp": day["fees_flat_15bp"],
            "fees_futubull": day["fees_futubull"],
            "hard_red": day["hard_red"],
            "picks": listed,
            "renewals": day["renewals"],
            "ret_flat_15bp": day["ret_flat_15bp"],
            "ret_futubull": day["ret_futubull"],
            "sells": day["sells"],
            "skips": day["skips"],
            "state": state,
            "ticker_pnl": day["ticker_pnl"],
        }
    baselines = _baselines(root, session, picks, bars, fees)
    blocked = [row["ticker"] for row in (bars.get("jumps") or []) if not row.get("explained")]
    return {
        "baselines": baselines,
        "fill_model": FILL_MODEL,
        "kind": "fills",
        "recipes": out,
        "session": session,
        "spec_sha256": spec["spec_sha256"],
        "study": STUDY_NAME,
        "unexplained": blocked,
    }


def _baselines(root: Path | None, session: str, picks: dict, bars: dict, fees: dict) -> dict:
    days = []
    for day in _sessions_of("fills", root):
        if day >= session:
            break
        days.append(day)
    days.append(session)
    packs = []
    for day in days:
        if day == session:
            doc_picks = picks
            doc_bars = bars
        else:
            doc_picks = _read_json(picks_path(root, day)) or {}
            doc_bars = _read_json(bar_path(root, day)) or {}
        opens, closes = _bars_map(doc_bars)
        packs.append({
            "closes": closes,
            "opens": opens,
            "session": day,
            "universe": list(doc_picks.get("universe") or []),
        })
    iwm = _iwm_path(packs, fees)
    draws = _random4_path(packs, fees)
    today_iwm = iwm[-1]
    today_draws = draws[-1]
    ordered = sorted(today_draws)
    mid = ordered[len(ordered) // 2] if ordered else 0.0
    return {
        "iwm_equity": today_iwm["equity"],
        "iwm_ret": today_iwm["ret"],
        "random4_draws": RANDOM_DRAWS,
        "random4_median": round(mid, 8),
        "random4_rets": [round(value, 8) for value in today_draws],
        "random4_seed": RANDOM_SEED,
    }


def _iwm_path(packs: list[dict], fees: dict) -> list[dict]:
    cash = CAPITAL
    shares = 0
    entry_fee = 0.0
    prev_eq = CAPITAL
    prev_mark = None
    out = []
    for i, pack in enumerate(packs):
        px_open = pack["opens"].get("IWM")
        px_close = pack["closes"].get("IWM")
        if i == 0:
            if px_open is None:
                raise ForwardError("IWM open missing")
            shares = int(CAPITAL // px_open)
            fee = order_fees(shares, px_open, "buy", fees)
            if shares * px_open + fee > cash + 1e-6:
                shares = int((cash - fee) // px_open) if px_open else 0
                fee = order_fees(shares, px_open, "buy", fees)
            cash -= shares * px_open + fee
            prev_mark = px_open
        mark = px_close if px_close is not None else prev_mark
        if mark is None:
            raise ForwardError("IWM mark missing")
        equity = round(cash + shares * float(mark), 4)
        ret = (equity / prev_eq - 1.0) if prev_eq else 0.0
        out.append({"equity": equity, "ret": round(ret, 8)})
        prev_eq = equity
        prev_mark = float(mark)
    return out


def _random4_path(packs: list[dict], fees: dict) -> list[list[float]]:
    """Hold-1 time-exit baseline. The strategy book stays keep-held."""
    books = []
    for draw in range(RANDOM_DRAWS):
        rng = random.Random(RANDOM_SEED + draw)
        cash = CAPITAL
        pos: dict[str, dict] = {}
        prev_eq = CAPITAL
        series = []
        for pack in packs:
            for ticker in list(pos):
                px = pack["opens"].get(ticker)
                if px is None or px <= 0:
                    continue
                lot = pos.pop(ticker)
                fee = order_fees(lot["shares"], px, "sell", fees)
                cash += lot["shares"] * px - fee
            pool = sorted(pack["universe"])
            k = min(4, len(pool))
            chosen = rng.sample(pool, k) if k else []
            if chosen and cash > 0:
                per = cash / len(chosen)
                for ticker in chosen:
                    px = pack["opens"].get(ticker)
                    if px is None or px <= 0 or ticker in pos:
                        continue
                    shares = int(per // px)
                    if shares < 1:
                        continue
                    fee = order_fees(shares, px, "buy", fees)
                    cost = shares * px + fee
                    if cost > cash + 1e-6:
                        shares = int((cash - fee) // px) if px else 0
                        if shares < 1:
                            continue
                        fee = order_fees(shares, px, "buy", fees)
                        cost = shares * px + fee
                    cash -= cost
                    pos[ticker] = {"prev": px, "shares": shares}
            stock = 0.0
            for ticker, lot in pos.items():
                mark = pack["closes"].get(ticker) or lot["prev"]
                stock += lot["shares"] * float(mark)
                lot["prev"] = float(mark)
            equity = round(cash + stock, 4)
            series.append((equity / prev_eq - 1.0) if prev_eq else 0.0)
            prev_eq = equity
        books.append(series)
    if not books:
        return [[] for _ in packs]
    return [[books[draw][i] for draw in range(RANDOM_DRAWS)] for i in range(len(packs))]


def rows_from_pin(picks: dict, *, blobs: dict[str, bytes] | None = None,
                  cwd: Path | None = None) -> list[dict]:
    """Rows from the pinned earliest blob. A later send file is not opened."""
    pin = picks.get("send_inputs") or None
    if not pin:
        return []
    blob = str(pin.get("blob_sha") or "")
    if blobs is not None and blob in blobs:
        raw = blobs[blob]
    else:
        proc = _git(["cat-file", "-p", blob], cwd or ROOT)
        if proc.returncode != 0:
            raise ForwardError("pinned send_inputs blob unreadable")
        raw = proc.stdout
    try:
        doc = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ForwardError("pinned send_inputs blob is not json") from exc
    return rows_for_record(str(picks.get("session") or ""), doc)


def record_fills(session: str, bars: dict, *, root: Path | None = None,
                 fees: dict | None = None, rows: list[dict] | None = None,
                 blobs: dict[str, bytes] | None = None) -> str:
    session = _check_session(session)
    spec = assert_spec_matches_group3(None) if root is None else load_spec(root)
    picks_file = picks_path(root, session)
    if not picks_file.is_file():
        raise ForwardError("fills require the morning picks file")
    for day in _sessions_of("picks", root):
        if day < session and not fills_path(root, day).is_file():
            raise ForwardError(f"{day} fills missing")
    prior = _sessions_of("fills", root)
    if prior and session <= prior[-1] and session not in prior:
        raise ForwardError("session not after the last recorded one")
    picks = json.loads(picks_file.read_text(encoding="utf-8"))
    if picks.get("fill_model") != FILL_MODEL:
        raise ForwardError("picks file is not keep-held")
    picks = dict(picks)
    if rows is None:
        rows = rows_from_pin(picks, blobs=blobs)
    if picks.get("sat_out") and any((slot.get("picks") or []) for slot in (picks.get("recipes") or {}).values()):
        raise ForwardError("a sit session has picks")
    picks["_rows"] = list(rows or [])
    fees = fees if fees is not None else load_fees()
    history = []
    for day in _sessions_of("fills", root):
        if day >= session:
            break
        doc = _read_json(bar_path(root, day))
        if doc:
            history.append(doc)
    doc = build_fills(session, picks, bars, fees, root, spec, history)
    doc.pop("_rows", None)
    path = fills_path(root, session)
    text = canon(doc) + "\n"
    bar_file = bar_path(root, session)
    bar_text = canon(bars) + "\n"
    if path.is_file():
        if path.read_bytes() != text.encode("utf-8"):
            raise ForwardError(f"existing fills changed ({session})")
        if bar_file.is_file() and bar_file.read_bytes() != bar_text.encode("utf-8"):
            raise ForwardError(f"existing bars changed ({session})")
        return "unchanged"
    if prior and session <= prior[-1]:
        raise ForwardError("session not after the last recorded one")
    if bar_file.is_file() and bar_file.read_bytes() != bar_text.encode("utf-8"):
        raise ForwardError(f"existing bars changed ({session})")
    _write_json(bar_file, bars)
    _write_json(path, doc)
    _append_manifest(root, _manifest_row(
        "fills", session, path, spec["spec_sha256"],
        {"bars": f"bars/{session}.json", "bars_sha256": sha256_bytes(bar_text.encode("utf-8"))},
    ))
    write_report(root)
    return "wrote"


def _pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%"


def _compound(values: list[float]) -> float:
    acc = 1.0
    for value in values:
        acc *= 1.0 + float(value)
    return acc - 1.0


def _ex_best(days: list[dict]) -> float | None:
    if not days:
        return None
    totals: dict[str, float] = {}
    first: dict[str, str] = {}
    for day in days:
        for ticker, pnl in (day.get("ticker_pnl") or {}).items():
            totals[ticker] = totals.get(ticker, 0.0) + float(pnl)
        for trade in day.get("buys") or []:
            first.setdefault(trade["ticker"], day["session"])
    if not totals:
        return _compound([day["ret_futubull"] for day in days])
    ranked = sorted(totals, key=lambda ticker: (-totals[ticker], first.get(ticker, "9999"), ticker))
    best = ranked[0]
    start = CAPITAL
    rets = []
    for day in days:
        end = float(day["equity_futubull"])
        removed = float((day.get("ticker_pnl") or {}).get(best) or 0.0)
        rets.append((end - removed) / start - 1.0 if start else 0.0)
        start = end
    return _compound(rets)


def collect_report(root: Path | None = None) -> dict:
    spec = load_spec(root)
    days = _sessions_of("fills", root)
    sleeves: dict[str, list[dict]] = {recipe_key(rec): [] for rec in spec["recipes"]}
    for winner in load_winners(root):
        sleeves.setdefault(winner["name"], [])
    iwm_rets = []
    draw_rets: list[list[float]] = []
    for day in days:
        doc = _read_json(fills_path(root, day)) or {}
        base = doc.get("baselines") or {}
        if "iwm_ret" in base:
            iwm_rets.append(float(base["iwm_ret"]))
        if base.get("random4_rets"):
            draw_rets.append([float(value) for value in base["random4_rets"]])
        for name, sleeve in (doc.get("recipes") or {}).items():
            sleeves.setdefault(name, []).append({
                "buys": sleeve.get("buys") or [],
                "closed_trades": sleeve.get("closed_trades") or [],
                "equity_futubull": sleeve.get("equity_futubull"),
                "ret_flat_15bp": sleeve.get("ret_flat_15bp"),
                "ret_futubull": sleeve.get("ret_futubull"),
                "session": day,
                "ticker_pnl": sleeve.get("ticker_pnl") or {},
            })
    rows = []
    iwm_compound = _compound(iwm_rets) if iwm_rets else None
    random_compounds = []
    if draw_rets:
        n = len(draw_rets[0])
        for draw in range(n):
            random_compounds.append(_compound([day[draw] for day in draw_rets]))
    random_median = None
    if random_compounds:
        ordered = sorted(random_compounds)
        random_median = ordered[len(ordered) // 2]
    for name, series in sleeves.items():
        trades = [trade for day in series for trade in day["closed_trades"]]
        wins = [trade for trade in trades if trade.get("win")]
        day_wins = [day for day in series if float(day["ret_futubull"]) > 0]
        compound = _compound([float(day["ret_futubull"]) for day in series]) if series else None
        flat = _compound([float(day["ret_flat_15bp"]) for day in series]) if series else None
        beaten = None
        if compound is not None and random_compounds:
            beaten = sum(1 for value in random_compounds if compound > value) / len(random_compounds)
        rows.append({
            "compound": compound,
            "ex_best": _ex_best(series) if series else None,
            "flat_15bp": flat,
            "iwm": iwm_compound,
            "name": name,
            "random4_beaten": beaten,
            "random4_median": random_median,
            "trades": len(trades),
            "win_days": len(day_wins),
            "win_rate": (len(wins) / len(trades)) if trades else None,
            "days": len(series),
        })
    return {"days": days, "fill_model": FILL_MODEL, "rows": rows, "spec_sha256": spec["spec_sha256"]}


def render_report(root: Path | None = None) -> str:
    payload = collect_report(root)
    lines = [
        f"# {STUDY_NAME}",
        "",
        f"Fill model: `{FILL_MODEL}`. Forward start {FORWARD_START}.",
        f"Spec sha256 `{payload['spec_sha256']}`.",
        "",
    ]
    if not payload["days"]:
        lines.append("No locked session yet. W/T days 0/0. Trades 0.")
        lines.append("")
    lines.append("| recipe | W/T days | trades | win rate | compound | flat 15bp | ex-best | IWM | RANDOM4 median | draws beaten |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in payload["rows"]:
        rate = "n/a" if row["win_rate"] is None else _pct(row["win_rate"])
        beaten = "n/a" if row["random4_beaten"] is None else _pct(row["random4_beaten"])
        lines.append(
            f"| `{row['name']}` | {row['win_days']}/{row['days']} | {row['trades']} | {rate} | "
            f"{_pct(row['compound'])} | {_pct(row['flat_15bp'])} | {_pct(row['ex_best'])} | "
            f"{_pct(row['iwm'])} | {_pct(row['random4_median'])} | {beaten} |"
        )
    lines.append("")
    lines.append(
        f"RANDOM4 seed {RANDOM_SEED}, {RANDOM_DRAWS} draws. "
        "IWM is buy-and-hold on the same locked sessions, Futubull fees."
    )
    lines.append("")
    return "\n".join(lines)


def write_report(root: Path | None = None) -> Path:
    path = report_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_report(root), encoding="utf-8")
    return path


def today_et() -> str:
    return datetime.now(ZoneInfo("America/New_York")).date().isoformat()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="forward_shadow_v1 research book")
    parser.add_argument("--check-spec", action="store_true")
    parser.add_argument("--picks", action="store_true")
    parser.add_argument("--fills", action="store_true")
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--date", default="")
    parser.add_argument("--rev", default="origin/main")
    args = parser.parse_args(argv)
    if args.check_spec or not (args.picks or args.fills or args.report):
        doc = assert_spec_matches_group3(None)
        print(f"spec {doc['spec_sha256']} fill_model {doc['fill_model']}")
        if args.check_spec and not (args.picks or args.fills):
            return
    if args.report and not (args.picks or args.fills):
        print(render_report(None), end="")
        return
    session = (args.date or today_et())[:10]
    if session < FORWARD_START:
        print(f"{session} is before {FORWARD_START}; nothing written")
        return
    if args.picks:
        loaded = earliest_send_inputs(session, rev=args.rev)
        status = record_picks(session, loaded=loaded, allow_missing=False)
        print(f"picks {session} {status}")
    if args.fills:
        if not picks_path(None, session).is_file():
            loaded = earliest_send_inputs(session, rev=args.rev)
            if loaded is None:
                print(f"fills {session} not a session")
                return
            print("picks", session, record_picks(session, loaded=loaded, allow_missing=True))
        from src.forward_shadow_v1_bars import pin_session

        picks = json.loads(picks_path(None, session).read_text(encoding="utf-8"))
        names = set(picks.get("universe") or [])
        names.add("IWM")
        for sleeve in (picks.get("recipes") or {}).values():
            names.update(sleeve.get("picks") or [])
        for day in _sessions_of("fills", None):
            if day >= session:
                break
            doc = _read_json(fills_path(None, day)) or {}
            for sleeve in (doc.get("recipes") or {}).values():
                for lot in (sleeve.get("state") or {}).get("positions") or []:
                    names.add(str(lot["ticker"]))
        bars = pin_session(session, sorted(names))
        print("fills", session, record_fills(session, bars))


if __name__ == "__main__":
    main()
