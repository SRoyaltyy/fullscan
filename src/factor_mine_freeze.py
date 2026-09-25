"""Append-only point-in-time inputs and decisions for Factor Mine.

A landed session is frozen once:

* ``data/factor_mine/snapshots/{D}.json`` — every row input used to
  decide D, including the heat vintage and the 09:30 open
* ``data/factor_mine/prices/{D}.json`` — prior bars and the session
  open that those inputs were scored from
* ``data/factor_mine/ledgers/{D}.json`` — buy/sell decisions for every
  recipe and every start-date book, plus the end-of-day state the next
  session resumes from
* ``data/factor_mine/freeze_manifest.json`` — sha256 of each file

Later runs read those files and append the new day. They do not rebuild
earlier dates. ``--restate D`` is the logged correction path.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
from datetime import datetime
from pathlib import Path

from . import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
SNAP_DIR = ROOT / "data" / "factor_mine" / "snapshots"
LEDGER_DIR = ROOT / "data" / "factor_mine" / "ledgers"
PRICE_DIR = ROOT / "data" / "factor_mine" / "prices"
MANIFEST_PATH = ROOT / "data" / "factor_mine" / "freeze_manifest.json"

_FILL_KEYS = (
    "date", "ticker", "side", "shares", "price", "fees", "pnl",
    "reason", "cash_after", "equity_after",
)


class HoldDay(Exception):
    """Do not land D. Missing bars would have been written as hot_score 0."""

    def __init__(self, date: str, missing: list[str] | None, reason: str):
        self.date = str(date)
        self.missing = [str(t) for t in (missing or [])][:80]
        self.reason = reason
        super().__init__(
            f"hold {self.date}: {reason}; "
            f"n={len(missing or [])} sample={self.missing[:12]}"
        )


class FrozenHistory(Exception):
    """A frozen snapshot or ledger already exists and was not restated."""


def canonical_bytes(obj) -> bytes:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        default=_json_default,
    ).encode("utf-8")


def _json_default(obj):
    item = getattr(obj, "item", None)
    if callable(item):
        try:
            value = item()
        except Exception:
            value = None
        else:
            if isinstance(value, float) and not math.isfinite(value):
                return None
            return value
    if isinstance(obj, float):
        return None if not math.isfinite(obj) else obj
    return str(obj)


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _now() -> str:
    return datetime.now(tl.ET).isoformat()


def load_manifest() -> dict:
    if not MANIFEST_PATH.is_file():
        return {
            "version": 1,
            "first_frozen": None,
            "snapshots": {},
            "ledgers": {},
            "prices": {},
            "restatements": [],
        }
    try:
        doc = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        doc = {}
    doc.setdefault("version", 1)
    doc.setdefault("first_frozen", None)
    doc.setdefault("snapshots", {})
    doc.setdefault("ledgers", {})
    doc.setdefault("prices", {})
    doc.setdefault("restatements", [])
    return doc


def save_manifest(doc: dict) -> None:
    snaps = doc.get("snapshots") or {}
    doc["first_frozen"] = min(snaps) if snaps else None
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(
        json.dumps(doc, indent=2, sort_keys=True), encoding="utf-8")


def snapshot_path(date: str) -> Path:
    return SNAP_DIR / f"{date}.json"


def ledger_path(date: str) -> Path:
    return LEDGER_DIR / f"{date}.json"


def price_path(date: str) -> Path:
    return PRICE_DIR / f"{date}.json"


def reset_price_memory() -> None:
    """Drop OHLC caches so a fetch just landed is visible to features()."""
    from . import candle_factor as cf
    from . import factor_mine as fm

    tl.reset_price_caches()
    cf._TICKER_BARS = None
    try:
        cf._bars_before.cache_clear()
    except Exception:
        pass
    fm._OHLC_CACHE.clear()
    fm._CANDLE_CACHE.clear()
    fm._SCAN_CACHE.clear()


def ranking_universe(date: str, cal: list[str], plan: dict,
                     movers: dict) -> list[str]:
    """Names whose bars must exist before D is ranked or written.

    Includes the liquid tape ``liquid_hot`` / ``continuation`` score,
    not only the names that survive a ranking done with empty bars.
    """
    from . import factor_mine as fm
    from . import gainer_asof as ga
    from . import gainer_capture as gc
    from . import oppset_clock_b as opp

    look = gc.lookback_calendar(cal)
    prior = gc.knowable_export_date(look, date)
    nxt = gc.next_session(cal, date)
    names: set[str] = set()
    for t in (plan or {}).get("tickers") or []:
        tick = fm._tick(t)
        if tick:
            names.add(tick)
    if prior:
        df = ga.load_finviz(prior)
        for raw in ga._liquid_tape(
            df, top_n=0, min_change=0.0, liquid=True,
            min_mcap_m=None, side="up", skip_change=True,
        ):
            tick = fm._tick((raw or {}).get("ticker"))
            if tick:
                names.add(tick)
        for raw in ga.liquid_gainers(df, top_n=60, min_change=0.0, liquid=True):
            tick = fm._tick((raw or {}).get("ticker"))
            if tick:
                names.add(tick)
        for t in gc.yesterday_gainers(prior, top_n=25):
            names.add(fm._tick(t))
        for t in gc.yesterday_movers(prior, top_n=20):
            names.add(fm._tick(t))
        for t in gc.earnings_reaction(prior, date):
            names.add(fm._tick(t))
    for t in gc.overnight_scheduled(prior, date, nxt):
        names.add(fm._tick(t))
    for t in gc.overnight_scheduled(
        prior, date, nxt, min_mcap_m=gc.OVERNIGHT_MEGA_MCAP_M,
    ):
        names.add(fm._tick(t))
    for t in (movers or {}).get(date) or []:
        names.add(fm._tick(t))
    if opp.union_enabled():
        for t in opp.flagged_tickers(date, top_n=30):
            names.add(fm._tick(t))
    names.discard("")
    return sorted(names)


def ensure_candidate_bars(date: str, tickers: list[str]) -> None:
    """Fetch bars for every candidate. Missing bars hold D."""
    from . import ohlc_ripper as ohlc
    from . import price_store as ps

    names = [t for t in tickers if t]
    if not names:
        raise HoldDay(date, [], "no candidates — refusing to freeze an empty day")
    try:
        ps.ensure_through(date, tickers=names)
    except (Exception, SystemExit) as e:
        raise HoldDay(date, names, f"price fetch failed: {e}") from e
    reset_price_memory()
    missing = [t for t in names if not ohlc.prior_bars(t, date, n=1)]
    if missing:
        raise HoldDay(
            date, missing,
            "no prior bars after fetch — refusing to write hot_score 0",
        )


def row_price_problem(ticker: str, date: str) -> str | None:
    """None when this row can be frozen. Otherwise it would be hot=0 or no open."""
    from . import ohlc_ripper as ohlc

    feat = ohlc.features(ticker, date)
    if not feat.get("ok"):
        return "hot_score unresolved"
    bar = tl.session_bar(ticker, date) or {}
    if bar.get("open") is None:
        return "missing 09:30 open"
    return None


def pin_morning_if_overlay(date: str) -> Path | None:
    """Copy a still-intact morning overlay aside so postclose cannot replace it."""
    from . import map_heat as mh

    dest = mh.OUT_DIR / f"{date}_map_heat_morning.json"
    if dest.is_file():
        return dest
    main = mh.OUT_DIR / f"{date}_map_heat.json"
    if not main.is_file():
        return None
    try:
        data = json.loads(main.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if str(data.get("phase") or "") != "morning_overlay":
        return None
    if not (data.get("tape") or []):
        return None
    dest.write_bytes(main.read_bytes())
    print(f"[factor-mine] pinned morning map_heat {dest.name}", flush=True)
    return dest


def heat_record(date: str, prior: str | None) -> dict:
    """Heat vintage knowable for D, preferring the immutable morning file."""
    pin_morning_if_overlay(date)
    board, vintage = tl._map_heat_board(date, prior)
    from . import map_heat as mh

    morning = mh.OUT_DIR / f"{date}_map_heat_morning.json"
    main = mh.OUT_DIR / f"{date}_map_heat.json"
    src = morning if morning.is_file() else main
    digest = None
    if src.is_file():
        try:
            digest = sha256_bytes(src.read_bytes())
        except OSError:
            digest = None
    return {
        "vintage": vintage,
        "phase": (board or {}).get("phase"),
        "board_date": vintage,
        "source": str(src.relative_to(ROOT)) if src.is_file() else None,
        "sha256": digest,
    }


def pin_prices(date: str, tickers: list[str]) -> dict:
    from . import ohlc_ripper as ohlc

    names = {}
    for t in sorted({str(x) for x in tickers if x}):
        sess = tl.session_bar(t, date) or {}
        names[t] = {
            "prior": ohlc.prior_bars(t, date, n=60),
            "open": sess.get("open"),
            "high": sess.get("high"),
            "low": sess.get("low"),
            "close": sess.get("close"),
        }
    return {"date": date, "names": names}


def bars_from_panel(panel: dict) -> dict:
    bars = {}
    for row in panel.get("rows") or []:
        t, d = row.get("ticker"), row.get("date")
        if not t or not d:
            continue
        bar = {}
        if row.get("open") is not None:
            bar["open"] = row.get("open")
        if row.get("close") is not None:
            bar["close"] = row.get("close")
        if bar:
            bars[(t, d)] = bar
    return bars


def bars_for_decisions(panel: dict, date: str, pinned: dict | None) -> dict:
    bars = bars_from_panel(panel)
    for t, info in ((pinned or {}).get("names") or {}).items():
        bars[(t, date)] = {
            "open": info.get("open"),
            "high": info.get("high"),
            "low": info.get("low"),
            "close": info.get("close"),
        }
    return bars


def _write_frozen(path: Path, obj: dict, slot: str, date: str, *,
                  restate: bool) -> str:
    raw = canonical_bytes(obj)
    digest = sha256_bytes(raw)
    man = load_manifest()
    prev = (man.get(slot) or {}).get(date) or {}
    if path.is_file() and not restate:
        have = sha256_bytes(path.read_bytes())
        raise FrozenHistory(
            f"{path.name} already frozen sha={have[:12]} "
            f"(pass --restate {date} to correct it)"
        )
    if path.is_file() and restate:
        have = sha256_bytes(path.read_bytes())
        man.setdefault("restatements", []).append({
            "date": date,
            "slot": slot,
            "at": _now(),
            "prev_sha256": prev.get("sha256") or have,
            "sha256": digest,
        })
        print(
            f"[factor-mine] RESTATE {slot} {date} "
            f"prev={ (prev.get('sha256') or have)[:12] } -> {digest[:12]}",
            flush=True,
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    man.setdefault(slot, {})[date] = {
        "sha256": digest,
        "written_at": _now(),
        "bytes": len(raw),
    }
    if slot == "snapshots":
        man[slot][date]["n_rows"] = len(obj.get("rows") or [])
        man[slot][date]["heat_vintage"] = (obj.get("heat") or {}).get("vintage")
    save_manifest(man)
    print(f"[factor-mine] froze {slot} {date} sha={digest[:12]}", flush=True)
    return digest


def write_snapshot(date: str, snap: dict, *, restate: bool = False) -> str:
    return _write_frozen(snapshot_path(date), snap, "snapshots", date, restate=restate)


def write_ledger(date: str, ledger: dict, *, restate: bool = False) -> str:
    return _write_frozen(ledger_path(date), ledger, "ledgers", date, restate=restate)


def write_price_pin(date: str, doc: dict, *, restate: bool = False) -> str:
    return _write_frozen(price_path(date), doc, "prices", date, restate=restate)


def read_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def apply_frozen_snapshots(panel: dict) -> dict:
    """Replace landed dates with their frozen rows. Other dates stay."""
    from . import factor_mine as fm

    panel = fm.rehydrate_panel(panel)
    if not SNAP_DIR.is_dir():
        return panel
    by_date = dict(panel.get("by_date") or {})
    replaced = False
    for path in sorted(SNAP_DIR.glob("*.json")):
        date = path.stem
        snap = read_json(path) or {}
        rows = list(snap.get("rows") or [])
        if not rows:
            continue
        by_date[date] = rows
        replaced = True
    if not replaced:
        return panel
    rows = []
    for date in sorted(by_date):
        rows.extend(by_date[date] or [])
    out = dict(panel)
    dates = list(panel.get("session_dates") or [])
    for date in by_date:
        if date not in dates:
            dates.append(date)
    dates = sorted({d for d in dates if d})
    out.update({
        "rows": rows,
        "by_date": by_date,
        "session_dates": dates,
        "n_rows": len(rows),
        "n_sessions": len(dates),
        "to_date": dates[-1] if dates else panel.get("to_date"),
    })
    return out


def make_snapshot(date: str, rows: list[dict], prior: str | None,
                  prices_sha: str | None) -> dict:
    heat = heat_record(date, prior)
    frozen_rows = []
    for row in rows:
        item = dict(row)
        item["heat_vintage"] = heat.get("vintage")
        item["open_0930"] = row.get("open")
        frozen_rows.append(item)
    frozen_rows.sort(key=lambda r: (
        r.get("date") or "", int(r.get("src_rank") or 0), r.get("ticker") or "",
    ))
    return {
        "date": date,
        "asof": "09:30_et",
        "open_clock": "09:30 ET",
        "heat": heat,
        "prices_sha256": prices_sha,
        "n_rows": len(frozen_rows),
        "rows": frozen_rows,
    }


def freeze_meta() -> dict:
    man = load_manifest()
    first = man.get("first_frozen")
    return {
        "first_frozen": first,
        "reconstructed_before": first,
        "n_snapshots": len(man.get("snapshots") or {}),
        "n_ledgers": len(man.get("ledgers") or {}),
    }


def label_payload(payload: dict) -> dict:
    """Mark sessions before the first frozen day as reconstructed."""
    meta = freeze_meta()
    payload = dict(payload)
    payload["freeze"] = meta
    first = meta.get("first_frozen")
    dates = list(payload.get("dates") or [])
    payload["reconstructed_dates"] = [
        d for d in dates if first and str(d) < str(first)
    ]
    return payload


def committed_manifest() -> dict:
    rel = MANIFEST_PATH.relative_to(ROOT).as_posix()
    try:
        out = subprocess.run(
            ["git", "show", f"HEAD:{rel}"],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
    except OSError:
        return {}
    if out.returncode != 0 or not (out.stdout or "").strip():
        return {}
    try:
        return json.loads(out.stdout)
    except json.JSONDecodeError:
        return {}


def guard_manifest(old: dict | None, new: dict | None,
                   restate: list[str] | None = None) -> None:
    """Fail if any earlier snapshot or ledger hash changed."""
    old = old or {}
    new = new or {}
    allow = {str(d)[:10] for d in (restate or []) if d}
    for slot in ("snapshots", "ledgers"):
        for date, meta in (old.get(slot) or {}).items():
            if date in allow:
                print(f"[factor-mine] restate allowed {slot} {date}", flush=True)
                continue
            now = (new.get(slot) or {}).get(date) or {}
            prev = (meta or {}).get("sha256")
            got = now.get("sha256")
            if prev and got != prev:
                raise SystemExit(
                    f"frozen {slot} {date} hash changed {prev} -> {got}. "
                    f"Pass --restate {date} to log a correction."
                )
    for slot, folder in (("snapshots", SNAP_DIR), ("ledgers", LEDGER_DIR)):
        for date, meta in (new.get(slot) or {}).items():
            path = folder / f"{date}.json"
            if not path.is_file():
                raise SystemExit(f"freeze manifest lists {slot} {date} but {path} is missing")
            have = sha256_bytes(path.read_bytes())
            if have != (meta or {}).get("sha256"):
                raise SystemExit(
                    f"freeze manifest {slot} {date} sha does not match {path.name}"
                )


def assert_history_unchanged(restate: list[str] | str | None = None) -> None:
    if isinstance(restate, str):
        restate = [d for d in restate.split(",") if d.strip()]
    env = os.environ.get("FM_RESTATE") or os.environ.get("RESTATE") or ""
    extra = [d.strip() for d in env.split(",") if d.strip()]
    guard_manifest(committed_manifest(), load_manifest(), list(restate or []) + extra)


def _prior(cal: list[str], date: str) -> str | None:
    prev = None
    for d in cal:
        if d == date:
            return prev
        if d < date:
            prev = d
    return prev


def _ledger_dates() -> list[str]:
    if not LEDGER_DIR.is_dir():
        return []
    return sorted(p.stem for p in LEDGER_DIR.glob("*.json"))


def latest_ledger_before(date: str) -> dict | None:
    prev = [d for d in _ledger_dates() if d < date]
    if not prev:
        return None
    return read_json(ledger_path(prev[-1]))


def _saved(ledger: dict | None, recipe: str, key: str) -> dict | None:
    if not ledger:
        return None
    rec = (ledger.get("recipes") or {}).get(recipe) or {}
    if key == "primary":
        return rec.get("primary")
    return (rec.get("starts") or {}).get(key)


def bridge_published(payload: dict, name: str, prior: str | None,
                     hold: int) -> dict | None:
    """Resume cursor from the published primary book. Does not resimulate it."""
    if not prior:
        return None
    daily = list((payload.get("daily") or {}).get(name) or [])
    row = next((d for d in daily if d.get("date") == prior), None)
    if row is None:
        earlier = [d for d in daily if str(d.get("date") or "") <= prior]
        row = earlier[-1] if earlier else None
    if not row:
        return None
    pos = {}
    for lot in row.get("lots") or []:
        t = str(lot.get("ticker") or "")
        if not t:
            continue
        try:
            px = float(lot.get("entry_px") or 0)
            shares = int(lot.get("shares") or 0)
        except (TypeError, ValueError):
            continue
        pos[t] = {
            "ticker": t,
            "shares": shares,
            "entry_px": px,
            "entry_date": lot.get("entry_date") or prior,
            "cost": shares * px,
            "fee_in": 0.0,
            "notional": shares * px,
            "last_px": px,
            "peak_px": px,
            "close_px": px,
            "min_hold": int(hold or 1),
        }
    return {
        "cash": row.get("cash"),
        "yday_equity": row.get("equity"),
        "pos": pos,
        "after": row.get("date"),
    }


def decision_from_book(book: dict, date: str) -> dict:
    trades = [t for t in (book.get("trades") or []) if t.get("date") == date]
    daily = next((d for d in (book.get("daily") or []) if d.get("date") == date), None)
    skips = [s for s in (book.get("skips") or []) if s.get("date") == date]

    def fill(t: dict) -> dict:
        return {k: t.get(k) for k in _FILL_KEYS if t.get(k) is not None}

    state = {
        "cash": book.get("cash"),
        "yday_equity": None if not daily else daily.get("equity"),
        "pos": book.get("pos") or {},
        "after": date,
    }
    if book.get("member_states") is not None:
        state["members"] = book.get("member_states")
    return {
        "buys": [fill(t) for t in trades if t.get("side") in ("BUY", "SHORT")],
        "sells": [fill(t) for t in trades if t.get("side") in ("SELL", "COVER")],
        "skips": [
            {k: s.get(k) for k in ("ticker", "kind", "reason") if s.get(k) is not None}
            for s in skips
        ],
        "daily": daily,
        "trades": trades,
        "state": state,
    }


def _simulate_single(panel, rec, *, start, bars, fees, regime, saved):
    from . import factor_mine_book as fmb

    resume = None
    if saved and saved.get("state"):
        resume = dict(saved["state"])
    if start:
        return fmb.simulate_book(
            panel, rec, start=start, bars=bars, fees=fees,
            regime=regime, resume=resume,
        )
    return fmb.simulate_book(
        panel, rec, bars=bars, fees=fees, regime=regime, resume=resume,
    )


def _simulate_combo(panel, spec, members, *, start, bars, fees, regime, saved):
    from . import factor_mine_combo as fmc

    resume = dict(saved["state"]) if saved and saved.get("state") else None
    pool = spec.get("pool") or "shared"
    weights = list(spec.get("weights") or [1] * len(members))
    if pool == "split":
        return fmc.simulate_split(
            panel, members, weights, start=start, bars=bars, fees=fees,
            regime=regime, name=spec.get("name") or "combo", resume=resume,
        )
    return fmc.simulate_shared(
        panel, members, weights, start=start, bars=bars, fees=fees,
        regime=regime, net=spec.get("net") or "priority",
        name=spec.get("name") or "combo", resume=resume,
    )


def build_ledger(panel: dict, payload: dict, recipes: list[dict],
                 date: str, bars: dict, *, fees=None, regime=None) -> dict:
    """Decisions for D only. Earlier days stay in the published payload."""
    from . import factor_mine as fm
    from . import factor_mine_book as fmb

    cal = [d for d in (panel.get("session_dates") or []) if d <= date]
    prior = _prior(cal, date)
    prev_ledger = latest_ledger_before(date)
    fees = fees if fees is not None else fm.pt_fees()
    regime = regime if regime is not None else fmb.load_regime()
    by_name = {r.get("name"): r for r in recipes if r.get("name")}
    out_recipes: dict[str, dict] = {}
    failed: list[str] = []

    singles = [
        r for r in recipes
        if r.get("name") and r.get("universe") != "combo" and not r.get("members")
    ]
    for rec in singles:
        name = rec["name"]
        try:
            saved = _saved(prev_ledger, name, "primary")
            if not saved:
                bridged = bridge_published(
                    payload, name, prior, int(rec.get("hold") or 1))
                if bridged:
                    saved = {"state": bridged}
                    print(f"[factor-mine] bridge {name} from published book @ {prior}",
                          flush=True)
            book = _simulate_single(
                panel, rec, start=None, bars=bars, fees=fees,
                regime=regime, saved=saved,
            )
            primary = decision_from_book(book, date)
            starts = {}
            origin = cal[0] if cal else date
            if origin:
                starts[origin] = primary
            for start in cal:
                if start == origin:
                    continue
                if start > date:
                    continue
                saved_s = _saved(prev_ledger, name, start)
                if start == date:
                    saved_s = None
                book_s = _simulate_single(
                    panel, rec, start=start, bars=bars, fees=fees,
                    regime=regime, saved=saved_s,
                )
                starts[start] = decision_from_book(book_s, date)
            out_recipes[name] = {"primary": primary, "starts": starts}
            print(f"[factor-mine] ledger {name} {date} "
                  f"buys={len(primary.get('buys') or [])} "
                  f"starts={len(starts)}", flush=True)
        except Exception as e:  # noqa: BLE001
            failed.append(name)
            print(f"[factor-mine] ledger failed {name}: {e}", flush=True)

    for rec in recipes:
        if rec.get("universe") != "combo" and not rec.get("members"):
            continue
        name = rec.get("name")
        if not name:
            continue
        members = []
        missing = False
        for member in rec.get("members") or []:
            hit = by_name.get(member)
            if not hit:
                missing = True
                break
            members.append(hit)
        if missing or not members:
            failed.append(name)
            print(f"[factor-mine] ledger failed combo {name}: missing members",
                  flush=True)
            continue
        spec = {
            "name": name,
            "members": list(rec.get("members") or []),
            "weights": list(rec.get("weights") or []),
            "net": rec.get("net") or "priority",
            "pool": rec.get("pool") or "shared",
        }
        try:
            saved = _saved(prev_ledger, name, "primary")
            book = _simulate_combo(
                panel, spec, members, start=None, bars=bars, fees=fees,
                regime=regime, saved=saved,
            )
            primary = decision_from_book(book, date)
            starts = {}
            origin = cal[0] if cal else None
            if origin:
                starts[origin] = primary
            for start in cal:
                if start == origin or start > date:
                    continue
                saved_s = _saved(prev_ledger, name, start)
                if start == date:
                    saved_s = None
                book_s = _simulate_combo(
                    panel, spec, members, start=start, bars=bars, fees=fees,
                    regime=regime, saved=saved_s,
                )
                starts[start] = decision_from_book(book_s, date)
            out_recipes[name] = {"primary": primary, "starts": starts}
        except Exception as e:  # noqa: BLE001
            failed.append(name)
            print(f"[factor-mine] ledger failed combo {name}: {e}", flush=True)

    if failed:
        raise HoldDay(
            date, failed,
            "ledger incomplete — refusing to freeze a partial decision set",
        )
    return {"date": date, "origin": "frozen", "recipes": out_recipes}


def _growth(decision: dict, prior_equity: float | None) -> float | None:
    row = decision.get("daily") or {}
    equity = row.get("equity")
    prev = row.get("yday_equity")
    if equity is None:
        return None
    base = prev if prev not in (None, 0) else prior_equity
    if not base:
        return None
    try:
        return float(equity) / float(base)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def splice_payload(payload: dict, date: str, ledger: dict, *,
                   replace: bool = False) -> dict:
    """Append D's frozen decisions. Earlier daily rows stay as they are."""
    from . import factor_mine as fm

    payload = dict(payload)
    dates = list(payload.get("dates") or [])
    if date not in dates:
        dates.append(date)
        dates.sort()
    payload["dates"] = dates
    payload["to_date"] = dates[-1] if dates else date
    payload["n_sessions"] = len(dates)
    daily_all = dict(payload.get("daily") or {})
    books = dict(payload.get("books") or {})
    series = dict(payload.get("series") or {})
    starts = dict(payload.get("starts") or {})
    capital = float(payload.get("capital") or fm.CAPITAL)

    for name, block in (ledger.get("recipes") or {}).items():
        primary = block.get("primary") or {}
        row = primary.get("daily")
        days = list(daily_all.get(name) or [])
        if replace:
            days = [d for d in days if d.get("date") != date]
        if row and not any(d.get("date") == date for d in days):
            days.append(fm._slim_dash_daily([row])[0])
        daily_all[name] = days
        book = dict(books.get(name) or {})
        trades = list(book.get("trades") or [])
        if replace:
            trades = [t for t in trades if t.get("date") != date]
        have = {(t.get("date"), t.get("side"), t.get("ticker")) for t in trades}
        for t in primary.get("trades") or []:
            key = (t.get("date"), t.get("side"), t.get("ticker"))
            if key not in have:
                trades.append(t)
                have.add(key)
        book["trades"] = trades
        state = primary.get("state") or {}
        if state.get("cash") is not None:
            book["cash"] = state.get("cash")
        book["n_trades"] = len([
            t for t in trades if t.get("side") not in ("OPEN", "CLOSE")
        ])
        books[name] = book
        eq = None if not row else row.get("equity")
        curve = list(series.get(name) or [])
        if eq is not None and (not curve or len(curve) < len(dates)):
            curve.append(eq)
        series[name] = curve
        if eq is not None:
            for stat in payload.get("stats") or []:
                if stat.get("name") == name:
                    stat["final_equity"] = eq
                    stat["total_ret_pct"] = round(100.0 * (float(eq) / capital - 1.0), 3)

        paths = list(starts.get(name) or [])
        by_start = {p.get("start"): p for p in paths}
        for start, decision in (block.get("starts") or {}).items():
            path = dict(by_start.get(start) or {"start": start, "days": []})
            sdays = list(path.get("days") or [])
            if replace:
                sdays = [d for d in sdays if d.get("date") != date]
            srow = decision.get("daily") or {}
            if srow and not any(d.get("date") == date for d in sdays):
                prev_eq = path.get("final_equity")
                if sdays and sdays[-1].get("equity") is not None:
                    prev_eq = sdays[-1].get("equity")
                growth = _growth(decision, prev_eq)
                new_eq = srow.get("equity")
                if prev_eq is not None and growth is not None and start != date:
                    new_eq = round(float(prev_eq) * growth, 2)
                sdays.append({
                    "date": date,
                    "s": srow.get("s"),
                    "hard_red": srow.get("hard_red"),
                    "bought": list(srow.get("bought") or []),
                    "sold": list(srow.get("sold") or []),
                    "cash": srow.get("cash"),
                    "equity": new_eq,
                    "open_cash": srow.get("open_cash"),
                    "made_money": bool(
                        growth is not None and growth > 1.0
                    ) if start != date else bool(srow.get("made_money")),
                })
            path["days"] = sdays
            path["n_sessions"] = len(sdays)
            if sdays and sdays[-1].get("equity") is not None:
                path["final_equity"] = sdays[-1]["equity"]
                try:
                    path["return_pct"] = round(
                        100.0 * (float(path["final_equity"]) / capital - 1.0), 3)
                except (TypeError, ValueError, ZeroDivisionError):
                    pass
                path["made_money"] = bool((path.get("return_pct") or 0) > 0)
            if start == date:
                path["bought"] = [b.get("ticker") for b in (decision.get("buys") or [])]
                path["buys"] = list(decision.get("buys") or [])
                path["pending"] = False
            path["start"] = start
            by_start[start] = path
        starts[name] = list(by_start.values())

    payload["daily"] = daily_all
    payload["books"] = books
    payload["series"] = series
    payload["starts"] = starts
    mornings = dict(payload.get("mornings") or {})
    if date not in mornings:
        try:
            from . import factor_mine_probe as fmp
            built = fmp.build_mornings()
            if isinstance(built, dict) and built:
                mornings = built
        except Exception as e:  # noqa: BLE001
            print(f"[factor-mine] mornings label skipped: {e}", flush=True)
    mornings.setdefault(date, {"s": None, "freeze": "appended"})
    payload["mornings"] = mornings
    return label_payload(payload)


def write_panel_file(panel: dict, path: Path | None = None) -> None:
    """Persist rows. Dates already on disk are kept if this panel omits them."""
    from . import factor_mine as fm

    path = Path(path or fm.PANEL_PATH)
    existing: dict = {}
    if path.is_file():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            existing = {}
    panel = fm.rehydrate_panel(panel)
    have = set(panel.get("session_dates") or [])
    old_rows = [
        r for r in (existing.get("rows") or [])
        if r.get("date") and r.get("date") not in have
    ]
    rows = list(old_rows) + list(panel.get("rows") or [])
    dates = sorted({
        d for d in list(existing.get("session_dates") or []) + list(have) + [
            r.get("date") for r in rows
        ] if d
    })
    slim = {k: v for k, v in panel.items() if k != "by_date"}
    slim.update({
        "from_date": existing.get("from_date") or panel.get("from_date"),
        "to_date": dates[-1] if dates else panel.get("to_date"),
        "session_dates": dates,
        "n_sessions": len(dates),
        "n_rows": len(rows),
        "rows": rows,
        "by_date": None,
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(slim, indent=2), encoding="utf-8")


def landed_dates(panel: dict) -> list[str]:
    return list(panel.get("session_dates") or [])


def append_land(from_date: str, target: str, *, write: bool = False,
                restate: list[str] | None = None,
                recipes: list[dict] | None = None,
                payload: dict | None = None,
                panel: dict | None = None) -> dict:
    """Land ``target`` from frozen history. Hold the day if bars are missing."""
    from . import factor_mine as fm

    restate_set = {str(d)[:10] for d in (restate or []) if d}
    if payload is None:
        payload = fm.load_scoreboard() if fm.OUT_JSON.is_file() else {}
    payload = dict(payload or {})
    if panel is None:
        if fm.PANEL_PATH.is_file():
            try:
                panel = json.loads(fm.PANEL_PATH.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                panel = {}
        else:
            panel = {}
    panel = apply_frozen_snapshots(fm.rehydrate_panel(panel or {}))
    have = set(landed_dates(panel))
    # Closed sessions strictly after the published board, plus restatements.
    published_dates = set(payload.get("dates") or [])
    want = fm.panel_lookback_calendar(from_date, target)
    new_dates = [
        d for d in want
        if d >= from_date and d <= target and (d not in have or d in restate_set)
        and (d not in published_dates or d in restate_set)
    ]
    # A date on the panel but not yet frozen still gets a snapshot when it
    # is the new session. Dates already published stay reconstructed.
    if target not in published_dates and target not in new_dates and target >= from_date:
        if target <= (want[-1] if want else target):
            new_dates.append(target)
    new_dates = sorted(set(new_dates))
    if not new_dates and target in published_dates and target not in restate_set:
        print(f"[factor-mine] freeze: {target} already published — no rewrite",
              flush=True)
        return label_payload(payload)

    frozen_dates = []
    for date in new_dates:
        if not fm.session_has_closed(date) and date not in restate_set:
            print(f"[factor-mine] freeze: {date} not closed — hold", flush=True)
            continue
        try:
            extra = fm.build_panel(date, date, fail_closed=True)
        except HoldDay as e:
            print(f"[factor-mine] {e}", flush=True)
            break
        panel = fm.merge_panel_days(panel, extra)
        rows = [
            r for r in (panel.get("rows") or []) if r.get("date") == date
        ]
        prior = _prior(list(panel.get("session_dates") or []), date)
        pinned = pin_prices(date, [r.get("ticker") for r in rows])
        try:
            prices_sha = write_price_pin(date, pinned, restate=date in restate_set)
        except FrozenHistory as e:
            print(f"[factor-mine] {e}", flush=True)
            prices_sha = (load_manifest().get("prices") or {}).get(date, {}).get("sha256")
            pinned = read_json(price_path(date)) or pinned
        snap = make_snapshot(date, rows, prior, prices_sha)
        try:
            write_snapshot(date, snap, restate=date in restate_set)
        except FrozenHistory as e:
            print(f"[factor-mine] {e}", flush=True)
        panel = apply_frozen_snapshots(panel)
        bars = bars_for_decisions(panel, date, pinned)
        recs = list(recipes or payload.get("recipes") or [])
        existing = read_json(ledger_path(date))
        if existing and date not in restate_set:
            ledger = existing
            print(f"[factor-mine] ledger {date} already frozen — splicing",
                  flush=True)
        else:
            ledger = build_ledger(panel, payload, recs, date, bars)
            try:
                write_ledger(date, ledger, restate=date in restate_set)
            except FrozenHistory as e:
                print(f"[factor-mine] {e}", flush=True)
                ledger = read_json(ledger_path(date)) or ledger
        payload = splice_payload(
            payload, date, ledger, replace=date in restate_set)
        payload["n_rows"] = panel.get("n_rows")
        frozen_dates.append(date)
        print(f"[factor-mine] appended frozen session {date}", flush=True)

    payload = label_payload(payload)
    if write and frozen_dates:
        write_panel_file(panel)
        fm.write_outputs(payload, stats=payload.get("stats") or [], books=payload.get("books"))
    elif write:
        print("[factor-mine] freeze: nothing new written", flush=True)
    return payload
