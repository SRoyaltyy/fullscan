"""Write-once rebuild of Factor Mine days that ran on an empty panel.

The 09:30 run saved ``source=no_same_day_panel`` and a 0-trade book when
that morning's rows were not in ``panel.json`` yet. Those sealed ledgers,
snapshots, candidate files, and state files stay as they are.
``data/factor_mine/rebuilt/DATE.json`` is a separate store. Each file is
written once.

A row is allowed when what it describes is the prior close or earlier, or
pre-09:30 ET that morning, even if the file was committed later. A row
that describes a moment after 09:30 ET that session is left out. The check
uses the panel's ``asof`` / ``leak`` fields and each row's ``prior_date``
and ``news_export_date``.

The candidate files from 2026-10-13 through 2026-11-02 are one copied list
of 1,401 names. Rebuilt days do not read that list. Names come from that
morning's prior-day export. A day with no export is flagged and is not
filled from the copied list.

Later sessions are chained forward from the rebuilt state, still inside
this store. ``src.factor_mine_past_lock --restate`` is not used and still
refuses a past day.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
REBUILT_DIR = ROOT / "data" / "factor_mine" / "rebuilt"
PANEL_REL = "data/factor_mine/panel.json"
LABELS_PAGE = ROOT / "dashboard" / "factor-mine" / "rebuilt_labels.json"
ET = ZoneInfo("America/New_York")
OPEN_MINUTES = 9 * 60 + 30
STUCK_N = 1401
STUCK_FROM = "2026-10-13"
STUCK_TO = "2026-11-02"
# Sources that are not the frozen liquid-tape candidate file.
MORNING_EXTRAS = frozenset({"flatten", "mover_buy"})
LABEL_REBUILT = "rebuilt"
LABEL_RULE = "sat out (rule)"
LABEL_MISSED = "missed"
BROKEN_STEPS = ("build", "lock", "render")


def rebuilt_path(date: str, root: Path | None = None) -> Path:
    return Path(root or ROOT) / "data" / "factor_mine" / "rebuilt" / f"{str(date)[:10]}.json"


def _day(value) -> str:
    return str(value or "")[:10]


def _load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _minutes(text: str) -> int | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    if raw in ("09:30_et", "09:30", "preopen", "prior_close", "prior"):
        return OPEN_MINUTES - 1
    if raw in ("16:00", "16:00_et", "close", "postclose", "after_open"):
        return 16 * 60
    stamp = raw.replace("Z", "+00:00")
    try:
        when = datetime.fromisoformat(stamp)
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=ET)
    local = when.astimezone(ET)
    return local.hour * 60 + local.minute


def describes_after_open(row: dict, session: str, *,
                         panel_asof: str = "",
                         panel_leak: str = "") -> str:
    """Reason the row describes a moment after 09:30 ET, else ``""``.

    Commit time is not an input. ``panel_asof`` / ``panel_leak`` are the
    panel document's fields. A row-level ``asof``, ``leak``, or
    ``described_at`` overrides them.
    """
    day = _day(session)
    asof = str(row.get("asof") or panel_asof or "")
    leak = str(row.get("leak") or panel_leak or "").lower()
    described = row.get("described_at") or row.get("asof_ts") or ""
    if described:
        minutes = _minutes(str(described))
        stamp = str(described).replace("Z", "+00:00")
        try:
            when = datetime.fromisoformat(stamp)
        except ValueError:
            when = None
        if when is not None:
            if when.tzinfo is None:
                when = when.replace(tzinfo=ET)
            local = when.astimezone(ET)
            if local.date().isoformat() > day or (
                local.date().isoformat() == day and minutes is not None
                and minutes >= OPEN_MINUTES
            ):
                return "described_at is after 09:30 ET"
        elif minutes is not None and minutes >= OPEN_MINUTES:
            return "described_at is after 09:30 ET"
    asof_minutes = _minutes(asof)
    if asof and asof_minutes is not None and asof_minutes >= OPEN_MINUTES:
        return f"asof={asof}"
    if any(token in leak for token in ("after 09:30", "post-open", "postclose", "same-day close")):
        return "leak marks a post-open description"
    prior = _day(row.get("prior_date"))
    news = _day(row.get("news_export_date"))
    if prior and prior > day:
        return "prior_date is after the session"
    if news and news > day:
        return "news_export_date is after the session"
    if news and news == day:
        return "same-day export describes the close, after 09:30 ET"
    row_date = _day(row.get("date") or day)
    if row_date > day:
        return "row date is after the session"
    return ""


def decision_row(row: dict) -> dict:
    """Copy a morning row without the same-day close.

    The close describes 16:00. The open is the 09:30 fill.
    """
    item = dict(row)
    item.pop("close", None)
    item["tag"] = LABEL_REBUILT
    return item


def leak_split(rows: list[dict], session: str, *,
               panel_asof: str = "", panel_leak: str = "") -> tuple[list[dict], list[dict]]:
    kept: list[dict] = []
    dropped: list[dict] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        reason = describes_after_open(
            row, session, panel_asof=panel_asof, panel_leak=panel_leak)
        if reason:
            dropped.append({
                "ticker": row.get("ticker"),
                "reason": reason,
                "tag": LABEL_REBUILT,
            })
            continue
        kept.append(decision_row(row))
    return kept, dropped


def no_same_day_dates(root: Path | None = None) -> dict[str, dict]:
    """Sealed send files whose source is ``no_same_day_panel``."""
    base = Path(root or ROOT) / "data" / "factor_mine" / "send_inputs"
    out: dict[str, dict] = {}
    if not base.is_dir():
        return out
    for path in sorted(base.glob("*.json")):
        doc = _load_json(path)
        if not isinstance(doc, dict):
            continue
        if doc.get("source") != "no_same_day_panel":
            continue
        day = _day(doc.get("date") or path.stem)
        out[day] = doc
    return out


def load_panel(root: Path | None = None) -> dict:
    path = Path(root or ROOT) / PANEL_REL
    doc = _load_json(path)
    return doc if isinstance(doc, dict) else {}


def rows_by_date(panel: dict) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for row in panel.get("rows") or []:
        if isinstance(row, dict):
            grouped.setdefault(_day(row.get("date")), []).append(row)
    return grouped


def matching_earlier_export(prior: str | None, digests: dict[str, str]) -> str | None:
    """Earlier session whose export file is the same bytes as ``prior``.

    A copied file is not that morning's list. The frozen 1,401-name
    candidate files are the same failure in another store.
    """
    day = _day(prior)
    digest = digests.get(day)
    if not day or not digest:
        return None
    for other in sorted(digests):
        if other >= day:
            break
        if digests[other] == digest:
            return other
    return None


def _export_digests(cal: list[str]) -> dict[str, str]:
    import hashlib

    from . import gainer_asof as ga

    out: dict[str, str] = {}
    for day in cal or []:
        path = ga.EXPORT_DIR / f"finviz_{_day(day)}.csv"
        if not path.is_file():
            continue
        out[_day(day)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return out


def _candidates_from_frame(morning: str, prior: str, frame, cal: list[str]) -> list[str]:
    """Same morning buckets as the frozen candidate file, from one frame."""
    from . import gainer_asof as ga
    from . import gainer_capture as gc
    from .factor_mine_freeze import MORNING_SOURCES, _ranked_names

    day = _day(morning)
    look = gc.lookback_calendar(list(cal or []))
    nxt = gc.next_session(look, day)
    buckets = {name: [] for name in MORNING_SOURCES}
    buckets["liquid_tape"] = _ranked_names(ga._liquid_tape(
        frame, top_n=0, min_change=0.0, liquid=True,
        min_mcap_m=None, side="up", skip_change=True,
    ))
    buckets["yday_gainer"] = _ranked_names(
        ga.liquid_gainers(frame, top_n=25, min_change=0.0, liquid=True))
    buckets["yday_mover"] = _ranked_names(ga.liquid_movers(frame, top_n=20, liquid=True))
    buckets["earn_react"] = _ranked_names(gc.earnings_reaction(prior, day, df=frame))
    buckets["overnight"] = _ranked_names(
        gc.overnight_scheduled(prior, day, nxt, df=frame))
    buckets["overnight_mega"] = _ranked_names(gc.overnight_scheduled(
        prior, day, nxt, df=frame, min_mcap_m=gc.OVERNIGHT_MEGA_MCAP_M,
    ))
    names: list[str] = []
    seen: set[str] = set()
    for source in MORNING_SOURCES:
        for tick in buckets.get(source) or []:
            if tick and tick not in seen:
                seen.add(tick)
                names.append(tick)
    return names


def _radar_candidates(morning: str, cal: list[str]) -> dict:
    """Prior session's theme-radar raw file. Never this morning's own file."""
    from .theme_radar_raw import SOURCE, load_prior_raw

    day = _day(morning)
    try:
        loaded = load_prior_raw(day)
    except Exception as exc:  # noqa: BLE001 — a bad snapshot is a flagged morning
        return {
            "date": day,
            "prior_export": None,
            "n": 0,
            "names": [],
            "reconstructed": False,
            "flag": f"theme-radar raw unusable: {exc}",
            "stuck_list_reused": False,
            "source": SOURCE,
        }
    prior = loaded["source_session"]
    names = _candidates_from_frame(day, prior, loaded["frame"], cal)
    flag = None if names else "theme-radar raw produced no names"
    loaded.pop("frame", None)
    return {
        "date": day,
        "prior_export": prior,
        "n": len(names),
        "names": names,
        "reconstructed": flag is None,
        "flag": flag,
        "stuck_list_reused": False,
        **loaded,
    }


def reconstruct_candidates(date: str, cal: list[str],
                           digests: dict[str, str] | None = None) -> dict:
    """Morning names from the prior export. Never the frozen candidate file.

    Frozen-copy mornings 2026-10-13 through 2026-11-04 read theme-radar's
    previous-session ``.raw.csv`` instead of the copied local export.
    """
    from .factor_mine_freeze import candidate_provenance
    from .theme_radar_raw import applies

    day = _day(date)
    if applies(day):
        return _radar_candidates(day, cal)
    try:
        doc = candidate_provenance(day, list(cal or []))
    except Exception as exc:  # noqa: BLE001 — a failed export is a flagged day
        return {
            "date": day,
            "prior_export": None,
            "n": 0,
            "names": [],
            "reconstructed": False,
            "flag": f"candidate reconstruction failed: {exc}",
            "stuck_list_reused": False,
        }
    names = [
        str(row.get("ticker"))
        for row in (doc.get("names") or [])
        if isinstance(row, dict) and row.get("ticker")
    ]
    prior = _day(doc.get("prior_export"))
    flag = None
    if not prior:
        flag = "no prior-day export"
    elif not names:
        flag = "prior-day export produced no names"
    else:
        copied = matching_earlier_export(prior, digests if digests is not None else _export_digests(cal))
        if copied:
            flag = (
                f"prior export {prior} is a byte copy of {copied}; "
                "that morning's candidate list was not reconstructed"
            )
            names = []
    return {
        "date": day,
        "prior_export": prior or None,
        "n": len(names),
        "names": names,
        "reconstructed": flag is None,
        "flag": flag,
        "stuck_list_reused": False,
    }


def _needs_export(rec: dict | None, spec: dict | None,
                 singles: dict[str, dict]) -> bool:
    """True when the recipe's names come from the prior-day export."""
    if spec is not None:
        return any(
            _needs_export(singles.get(member), None, singles)
            for member in (spec.get("members") or [])
        )
    uni = (rec or {}).get("universe") or "union"
    return uni not in MORNING_EXTRAS


def select_rows(rows: list[dict], candidates: dict) -> list[dict]:
    """Keep reconstructed names plus flatten / mover rows.

    The copied 1,401-name file is not a source. When the export cannot
    be reconstructed, only the extra morning lists stay.
    """
    names = set(candidates.get("names") or [])
    ok = bool(candidates.get("reconstructed"))
    kept = []
    for row in rows or []:
        ticker = str(row.get("ticker") or "")
        sources = set(row.get("sources") or [])
        if ok and ticker in names:
            kept.append(row)
            continue
        if sources and sources <= MORNING_EXTRAS:
            kept.append(row)
    return kept


def both_halves_zero(daily: dict | None) -> bool:
    if not isinstance(daily, dict):
        return False
    try:
        overnight = float(daily.get("overnight_delta") or 0)
        session = float(daily.get("session_delta") or 0)
    except (TypeError, ValueError):
        return False
    return abs(overnight) < 1e-9 and abs(session) < 1e-9


def _hard_red(daily: dict | None, skips: list[dict] | None) -> bool:
    if isinstance(daily, dict) and daily.get("hard_red"):
        return True
    raw = None if not isinstance(daily, dict) else daily.get("s")
    try:
        if raw is not None and float(raw) <= -3:
            return True
    except (TypeError, ValueError):
        pass
    return any(str((row or {}).get("kind") or "") == "hard_red" for row in skips or [])


def classify_zero(*, hard_red: bool, n_picks: int, skips: list[dict] | None,
                  both_zero: bool, empty_input: bool, candidate_ok: bool,
                  universe_rows: int, sim_error: str = "",
                  rebuilt_fills: int = 0) -> dict:
    """One both-halves-0 day: rebuilt, sat out (rule), or still broken."""
    kinds = {str((row or {}).get("kind") or "") for row in skips or []}
    kinds.discard("")
    if sim_error:
        return {"class": "still broken", "step": "render", "reason": sim_error}
    if hard_red:
        return {
            "class": LABEL_RULE,
            "step": None,
            "reason": "hard-red sit S<=-3",
        }
    if empty_input and not candidate_ok and universe_rows == 0 and rebuilt_fills == 0:
        return {
            "class": "still broken",
            "step": "build",
            "reason": "candidates could not be reconstructed from the prior-day export",
        }
    if "no_price" in kinds and rebuilt_fills == 0:
        return {
            "class": "still broken",
            "step": "lock",
            "reason": "picks had no 09:30 open",
        }
    if both_zero and n_picks == 0 and not kinds:
        if universe_rows > 0 and not empty_input:
            return {
                "class": "still broken",
                "step": "build",
                "reason": "sealed day picked nobody while leak-safe names existed",
            }
        if universe_rows == 0 and not candidate_ok and empty_input:
            return {
                "class": "still broken",
                "step": "build",
                "reason": "no leak-safe rows and the morning list was not reconstructed",
            }
        return {
            "class": LABEL_RULE,
            "step": None,
            "reason": "strategy no-trade rule",
        }
    if rebuilt_fills or (empty_input and not both_zero):
        return {
            "class": LABEL_REBUILT,
            "step": None,
            "reason": "rebuilt from the prior day's state at the session open",
        }
    if both_zero and empty_input:
        return {
            "class": LABEL_RULE,
            "step": None,
            "reason": "strategy no-trade rule",
        }
    if both_zero:
        return {
            "class": LABEL_RULE,
            "step": None,
            "reason": "strategy no-trade rule",
        }
    return {
        "class": LABEL_REBUILT,
        "step": None,
        "reason": "rebuilt from the prior day's state at the session open",
    }


def _universe_count(rows: list[dict], rec: dict | None) -> int:
    if not rec or rec.get("members"):
        return len(rows or [])
    uni = rec.get("universe") or "union"
    if uni in ("union", "combo"):
        return len(rows or [])
    return sum(1 for row in rows or [] if uni in set(row.get("sources") or []))


def _radar_stamp(candidates: dict) -> dict:
    if candidates.get("source") != "theme_radar_snapshot":
        return {}
    return {
        "source": "theme_radar_snapshot",
        "source_file": candidates.get("source_file"),
        "source_sha256": candidates.get("source_sha256"),
        "source_session": candidates.get("source_session"),
        "source_scrape_ts_utc": candidates.get("source_scrape_ts_utc"),
    }


def _slim_fill(trade: dict, radar: dict | None = None) -> dict:
    keep = ("date", "ticker", "side", "shares", "price", "fees", "pnl", "reason")
    out = {key: trade.get(key) for key in keep if trade.get(key) is not None}
    out["tag"] = LABEL_REBUILT
    out.update(radar or {})
    return out


def _slim_daily(daily: dict | None) -> dict:
    if not isinstance(daily, dict):
        return {}
    keep = (
        "date", "s", "hard_red", "n", "open_equity", "yday_equity",
        "overnight_delta", "session_delta", "equity", "cash",
        "bought", "sold", "open_held",
    )
    out = {key: daily.get(key) for key in keep if key in daily}
    out["tag"] = LABEL_REBUILT
    return out


def _pct(daily: dict | None) -> float | None:
    if not isinstance(daily, dict):
        return None
    try:
        equity = float(daily.get("equity"))
        base = float(daily.get("yday_equity"))
    except (TypeError, ValueError):
        return None
    if not base:
        return None
    return round(100.0 * (equity / base - 1.0), 4)


def live_names(root: Path | None = None) -> list[str]:
    """Lineup on the latest sealed session, plus the send-file live recipes."""
    from .factor_mine_send_inputs import LIVE_RECIPES

    base = Path(root or ROOT) / "data" / "factor_mine" / "lineups"
    names: list[str] = []
    if base.is_dir():
        files = sorted(path for path in base.glob("*.json") if path.stem[:1].isdigit())
        if files:
            doc = _load_json(files[-1]) or {}
            for row in doc.get("recipes") or []:
                name = str((row or {}).get("name") or "")
                if name and name not in names:
                    names.append(name)
    for name in LIVE_RECIPES:
        if name not in names:
            names.append(name)
    return names


def _recipe_index() -> tuple[dict[str, dict], dict[str, dict]]:
    from . import factor_mine as fm
    from . import factor_mine_combo as fmc

    singles = {rec["name"]: rec for rec in fm.build_recipes() if rec.get("name")}
    combos = {spec["name"]: spec for spec in fmc.combo_specs() if spec.get("name")}
    return singles, combos


_LEDGER_MEMO: dict[tuple[str, str], dict | None] = {}


def _ledger(date: str, root: Path | None = None) -> dict | None:
    from . import factor_mine_freeze as fmf

    day = _day(date)
    base = str(Path(root or ROOT))
    key = (base, day)
    if key in _LEDGER_MEMO:
        return _LEDGER_MEMO[key]
    saved = fmf.LEDGER_DIR
    if root is not None:
        fmf.LEDGER_DIR = Path(root) / "data" / "factor_mine" / "ledgers"
    try:
        doc = fmf.read_ledger(day)
    finally:
        fmf.LEDGER_DIR = saved
    _LEDGER_MEMO[key] = doc
    return doc


def _sealed_state(ledger: dict | None, name: str) -> dict | None:
    if not ledger:
        return None
    primary = ((ledger.get("recipes") or {}).get(name) or {}).get("primary") or {}
    state = primary.get("state")
    return dict(state) if isinstance(state, dict) else None


def _prior_session(date: str, cal: list[str]) -> str | None:
    earlier = [day for day in cal if day < _day(date)]
    return earlier[-1] if earlier else None


def _resume_for(name: str, prior: str | None, chain: dict[str, dict],
                root: Path | None) -> dict | None:
    if prior:
        carried = ((chain.get(prior) or {}).get("strategies") or {}).get(name) or {}
        state = carried.get("state")
        if isinstance(state, dict) and state:
            return state
        sealed = _sealed_state(_ledger(prior, root), name)
        if sealed:
            return sealed
    return None


def _simulate(name: str, rec: dict | None, spec: dict | None,
              panel: dict, resume: dict | None, singles: dict[str, dict],
              *, fees: dict, regime: dict) -> dict:
    from . import factor_mine_freeze as fmf

    saved = {"state": resume} if resume else None
    if spec is not None:
        members = []
        for member in spec.get("members") or []:
            hit = singles.get(member)
            if not hit:
                raise KeyError(f"missing member {member}")
            members.append(hit)
        book = fmf._simulate_combo(
            panel, spec, members, start=None, bars=None, fees=fees,
            regime=regime, saved=saved,
        )
    else:
        book = fmf._simulate_single(
            panel, rec, start=None, bars=None, fees=fees,
            regime=regime, saved=saved,
        )
    return fmf.decision_from_book(book, panel["session_dates"][-1])


def _day_panel(date: str, rows: list[dict], panel: dict) -> dict:
    return {
        "session_dates": [date],
        "rows": rows,
        "by_date": {date: rows},
        "asof": panel.get("asof") or "09:30_et",
        "leak": panel.get("leak") or "",
        "_ohlc_filled": True,
        "_tape_filled": True,
        "_clock_b": True,
        "_oppset": True,
    }


def _write_once(path: Path, doc: dict) -> bool:
    """True when the file was created. An existing file is left untouched."""
    if path.is_file():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(doc, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return True


def build_day(date: str, *, panel: dict, grouped: dict[str, list[dict]],
              cal: list[str], names: list[str], singles: dict[str, dict],
              combos: dict[str, dict], chain: dict[str, dict],
              empty_inputs: dict[str, dict], fees: dict, regime: dict,
              digests: dict[str, str] | None = None,
              root: Path | None = None) -> dict:
    """One rebuilt session. Does not read or write a sealed file."""
    day = _day(date)
    raw_rows = list(grouped.get(day) or [])
    kept, dropped = leak_split(
        raw_rows, day,
        panel_asof=str(panel.get("asof") or ""),
        panel_leak=str(panel.get("leak") or ""),
    )
    candidates = reconstruct_candidates(day, cal, digests)
    chosen_rows = select_rows(kept, candidates)
    mini = _day_panel(day, chosen_rows, panel)
    prior = _prior_session(day, cal)
    empty = day in empty_inputs
    strategies: dict[str, dict] = {}
    for name in names:
        rec = singles.get(name)
        spec = None if rec else combos.get(name)
        resume = _resume_for(name, prior, chain, root)
        entry: dict = {
            "name": name,
            "tag": LABEL_REBUILT,
            "picks": [],
            "fills": [],
            "skips": [],
            "daily": {},
            "state": None,
        }
        if rec is None and spec is None:
            verdict = {
                "class": "still broken",
                "step": "build",
                "reason": "live recipe is not in the catalog",
            }
            entry.update(verdict)
            entry.update(_radar_stamp(candidates))
            strategies[name] = entry
            continue
        needs_export = _needs_export(rec, spec, singles)
        if needs_export and not candidates.get("reconstructed"):
            verdict = {
                "class": "still broken",
                "step": "build",
                "reason": candidates.get("flag") or "morning candidates were not reconstructed",
            }
            entry.update(verdict)
            entry.update(_radar_stamp(candidates))
            if resume:
                entry["state"] = resume
            strategies[name] = entry
            continue
        try:
            decision = _simulate(
                name, rec, spec, mini, resume, singles,
                fees=fees, regime=regime,
            )
        except Exception as exc:  # noqa: BLE001 — record the step, do not touch sealed books
            verdict = classify_zero(
                hard_red=False, n_picks=0, skips=[], both_zero=True,
                empty_input=empty, candidate_ok=bool(candidates.get("reconstructed")),
                universe_rows=_universe_count(chosen_rows, rec),
                sim_error=f"{type(exc).__name__}: {exc}",
            )
            entry.update(verdict)
            entry.update(_radar_stamp(candidates))
            if resume:
                entry["state"] = resume
            strategies[name] = entry
            continue
        daily = decision.get("daily") or {}
        radar = _radar_stamp(candidates)
        fills = [
            _slim_fill(trade, radar)
            for trade in (decision.get("trades") or [])
            if trade.get("side") in ("BUY", "SELL", "SHORT", "COVER")
        ]
        skips = list(decision.get("skips") or [])
        picks = [
            str(trade.get("ticker"))
            for trade in (decision.get("buys") or [])
            if trade.get("ticker")
        ]
        if not picks:
            picks = [str(name_) for name_ in (daily.get("bought") or []) if name_]
        zero = both_halves_zero(daily)
        verdict = classify_zero(
            hard_red=_hard_red(daily, skips),
            n_picks=int(daily.get("n") or 0),
            skips=skips,
            both_zero=zero,
            empty_input=empty,
            candidate_ok=bool(candidates.get("reconstructed")),
            universe_rows=_universe_count(chosen_rows, rec),
            rebuilt_fills=len(fills),
        )
        if empty and not chosen_rows and not _hard_red(daily, skips) and not fills:
            verdict = {
                "class": "still broken",
                "step": "build",
                "reason": "no leak-safe panel rows for this session",
            }
        elif not empty and not zero and verdict["class"] != "still broken":
            verdict = {
                "class": LABEL_REBUILT,
                "step": None,
                "reason": "chained forward from the rebuilt state",
            }
        entry.update({
            "picks": picks,
            "fills": fills,
            "skips": [
                {"ticker": row.get("ticker"), "kind": row.get("kind"),
                 "reason": row.get("reason"), "tag": LABEL_REBUILT, **radar}
                for row in skips if isinstance(row, dict)
            ],
            "daily": {**_slim_daily(daily), **radar},
            "state": decision.get("state"),
            "return_pct": _pct(daily),
        })
        entry.update(verdict)
        entry.update(_radar_stamp(candidates))
        strategies[name] = entry
    radar_day = _radar_stamp(candidates)
    return {
        "date": day,
        "tag": LABEL_REBUILT,
        **radar_day,
        "empty_input": empty,
        "sealed_source": "no_same_day_panel" if empty else "panel",
        "chain_from": prior,
        "panel_asof": panel.get("asof") or "",
        "panel_leak": panel.get("leak") or "",
        "candidates": {
            "prior_export": candidates.get("prior_export"),
            "n": candidates.get("n"),
            "reconstructed": candidates.get("reconstructed"),
            "flag": candidates.get("flag"),
            "stuck_list_reused": False,
            "names": candidates.get("names") or [],
            "source": candidates.get("source"),
            "source_file": candidates.get("source_file"),
            "source_sha256": candidates.get("source_sha256"),
        },
        "leak": {
            "kept": len(kept),
            "excluded": len(dropped),
            "used": len(chosen_rows),
            "excluded_rows": dropped,
        },
        "strategies": strategies,
    }


def calendar(panel: dict, empty: dict[str, dict], root: Path | None = None) -> list[str]:
    days = set(panel.get("session_dates") or [])
    days.update(empty)
    ledger_dir = Path(root or ROOT) / "data" / "factor_mine" / "ledgers"
    if ledger_dir.is_dir():
        for path in ledger_dir.glob("*.json.gz"):
            days.add(path.name[:10])
    return sorted(day for day in days if len(day) == 10)


def _replace_with_radar(day: str, doc: dict | None) -> bool:
    """True when a frozen-copy morning still lacks a theme-radar rebuild."""
    from .theme_radar_raw import applies

    if not applies(day):
        return False
    return (doc or {}).get("source") != "theme_radar_snapshot"


def run_rebuild(*, root: Path | None = None, write: bool = False,
                through: str | None = None) -> dict:
    """Rebuild empty-input days and chain later sessions forward.

    ``write`` creates missing ``rebuilt/DATE.json`` files. Existing files
    stay byte-for-byte. Sealed paths are not opened for writing.
    """
    base = Path(root or ROOT)
    panel = load_panel(base)
    grouped = rows_by_date(panel)
    empty = no_same_day_dates(base)
    cal = calendar(panel, empty, base)
    if not empty or not cal:
        return {"dates": [], "wrote": [], "skipped": []}
    first = min(empty)
    last = through or cal[-1]
    window = [day for day in cal if first <= day <= last]
    names = live_names(base)
    singles, combos = _recipe_index()
    from . import factor_mine as fm
    from . import factor_mine_book as fmb
    fees = fm.pt_fees()
    regime = fmb.load_regime()
    digests = _export_digests(cal)
    chain: dict[str, dict] = {}
    wrote: list[str] = []
    skipped: list[str] = []
    for day in window:
        path = rebuilt_path(day, base)
        replacing = False
        if path.is_file():
            have = _load_json(path)
            if not _replace_with_radar(day, have if isinstance(have, dict) else None):
                if isinstance(have, dict):
                    chain[day] = have
                skipped.append(day)
                continue
            replacing = True
        doc = build_day(
            day, panel=panel, grouped=grouped, cal=cal, names=names,
            singles=singles, combos=combos, chain=chain, empty_inputs=empty,
            fees=fees, regime=regime, digests=digests, root=base,
        )
        chain[day] = doc
        if write:
            if replacing:
                path.write_text(
                    json.dumps(doc, indent=2, sort_keys=True, default=str) + "\n",
                    encoding="utf-8",
                )
                wrote.append(day)
            elif _write_once(path, doc):
                wrote.append(day)
        else:
            wrote.append(day)
        print(
            f"[factor-mine-rebuild] {day} empty={doc['empty_input']} "
            f"candidates={doc['candidates'].get('n')} "
            f"flag={doc['candidates'].get('flag')} "
            f"kept={doc['leak']['used']} excluded={doc['leak']['excluded']}",
            flush=True,
        )
    return {"dates": window, "wrote": wrote, "skipped": skipped, "docs": chain}


def _sealed_zero_rows(root: Path) -> list[dict]:
    """Both-halves-0 primary days on the live lineup. Read-only."""
    import gzip

    names = live_names(root)
    singles, combos = _recipe_index()
    ledger_dir = root / "data" / "factor_mine" / "ledgers"
    rows = []
    if not ledger_dir.is_dir():
        return rows
    for path in sorted(ledger_dir.glob("*.json.gz")):
        try:
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                ledger = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(ledger, dict):
            continue
        day = _day(ledger.get("date") or path.name)
        recipes = ledger.get("recipes") or {}
        for name in names:
            primary = (recipes.get(name) or {}).get("primary") or {}
            daily = primary.get("daily") or {}
            if not both_halves_zero(daily):
                continue
            rows.append({
                "date": day,
                "name": name,
                "daily": daily,
                "skips": list(primary.get("skips") or []),
                "rec": singles.get(name),
                "combo": name in combos,
            })
    return rows


def classify_books(root: Path | None = None, docs: dict | None = None) -> dict:
    """Label every sealed both-halves-0 day on the live lineup."""
    base = Path(root or ROOT)
    panel = load_panel(base)
    grouped = rows_by_date(panel)
    empty = set(no_same_day_dates(base))
    cal = calendar(panel, {day: {} for day in empty}, base)
    cache: dict[str, dict] = {}
    digests = _export_digests(cal)
    if docs is None:
        rebuilt_root = base / "data" / "factor_mine" / "rebuilt"
        loaded = {}
        if rebuilt_root.is_dir():
            for path in rebuilt_root.glob("*.json"):
                if path.stem in ("labels", "counts"):
                    continue
                doc = _load_json(path)
                if isinstance(doc, dict) and doc.get("date"):
                    loaded[_day(doc["date"])] = doc
        docs = loaded
    counts: dict[str, dict] = {}
    cells = []
    for row in _sealed_zero_rows(base):
        day = row["date"]
        name = row["name"]
        rebuilt = (((docs.get(day) or {}).get("strategies") or {}).get(name) or {})
        if rebuilt.get("class"):
            verdict = {
                "class": rebuilt.get("class"),
                "step": rebuilt.get("step"),
                "reason": rebuilt.get("reason") or "",
            }
            cells.append({
                "strategy": name,
                "date": day,
                "label": LABEL_MISSED if verdict["class"] == "still broken" else verdict["class"],
                "class": verdict["class"],
                "step": verdict.get("step"),
                "reason": verdict.get("reason") or "",
                "empty_input": day in empty,
                "candidate_flag": ((docs.get(day) or {}).get("candidates") or {}).get("flag"),
            })
            bucket = counts.setdefault(name, {
                "both_halves_0": 0, "rebuilt": 0, "sat out (rule)": 0,
                "still broken": 0, "build": 0, "lock": 0, "render": 0,
            })
            bucket["both_halves_0"] += 1
            if verdict["class"] in bucket:
                bucket[verdict["class"]] += 1
            step = verdict.get("step")
            if step in bucket:
                bucket[step] += 1
            continue
        if day not in cache:
            kept, _dropped = leak_split(
                grouped.get(day) or [], day,
                panel_asof=str(panel.get("asof") or ""),
                panel_leak=str(panel.get("leak") or ""),
            )
            candidates = reconstruct_candidates(day, cal, digests)
            cache[day] = {
                "rows": select_rows(kept, candidates),
                "candidates": candidates,
            }
        packed = cache[day]
        verdict = classify_zero(
                hard_red=_hard_red(row["daily"], row["skips"]),
                n_picks=int((row["daily"] or {}).get("n") or 0),
                skips=row["skips"],
                both_zero=True,
                empty_input=day in empty,
                candidate_ok=bool(packed["candidates"].get("reconstructed")),
                universe_rows=_universe_count(packed["rows"], row["rec"]),
            )
        label = verdict["class"]
        if label == "still broken":
            shown = LABEL_MISSED
        else:
            shown = label
        bucket = counts.setdefault(name, {
            "both_halves_0": 0, "rebuilt": 0, "sat out (rule)": 0,
            "still broken": 0, "build": 0, "lock": 0, "render": 0,
        })
        bucket["both_halves_0"] += 1
        if label in bucket:
            bucket[label] += 1
        step = verdict.get("step")
        if step in bucket:
            bucket[step] += 1
        cells.append({
            "strategy": name,
            "date": day,
            "label": shown,
            "class": label,
            "step": step,
            "reason": verdict.get("reason") or "",
            "empty_input": day in empty,
            "candidate_flag": (cache[day]["candidates"].get("flag")),
        })
    return {"counts": counts, "cells": cells}


def labels_document(report: dict) -> dict:
    by_strategy: dict[str, dict] = {}
    for cell in report.get("cells") or []:
        by_strategy.setdefault(cell["strategy"], {})[cell["date"]] = {
            "label": cell["label"],
            "class": cell["class"],
            "step": cell.get("step"),
            "reason": cell.get("reason") or "",
        }
    return {
        "tag": LABEL_REBUILT,
        "fill": "forward",
        "strategies": by_strategy,
    }


def counts_table(report: dict) -> str:
    headers = (
        "strategy", "both-halves-0", "rebuilt", "sat out (rule)",
        "still broken", "build", "lock", "render",
    )
    lines = ["| " + " | ".join(headers) + " |",
             "| " + " | ".join("---" for _ in headers) + " |"]
    totals = {key: 0 for key in headers[1:]}
    for name in sorted((report.get("counts") or {})):
        row = report["counts"][name]
        values = [
            name,
            str(row.get("both_halves_0") or 0),
            str(row.get("rebuilt") or 0),
            str(row.get("sat out (rule)") or 0),
            str(row.get("still broken") or 0),
            str(row.get("build") or 0),
            str(row.get("lock") or 0),
            str(row.get("render") or 0),
        ]
        lines.append("| " + " | ".join(values) + " |")
        totals["both-halves-0"] += row.get("both_halves_0") or 0
        totals["rebuilt"] += row.get("rebuilt") or 0
        totals["sat out (rule)"] += row.get("sat out (rule)") or 0
        totals["still broken"] += row.get("still broken") or 0
        for step in BROKEN_STEPS:
            totals[step] += row.get(step) or 0
    lines.append("| " + " | ".join(
        ["total"] + [str(totals[key]) for key in headers[1:]]
    ) + " |")
    return "\n".join(lines)


def write_labels(report: dict, root: Path | None = None) -> dict[str, Path]:
    base = Path(root or ROOT)
    payload = labels_document(report)
    rebuilt = base / "data" / "factor_mine" / "rebuilt"
    rebuilt.mkdir(parents=True, exist_ok=True)
    labels = rebuilt / "labels.json"
    counts = rebuilt / "counts.json"
    text = json.dumps(payload, indent=2, sort_keys=True)
    labels.write_text(text + "\n", encoding="utf-8")
    counts.write_text(
        json.dumps({
            "counts": report.get("counts") or {},
            "table": counts_table(report),
        }, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    page = base / "dashboard" / "factor-mine" / "rebuilt_labels.json"
    page.parent.mkdir(parents=True, exist_ok=True)
    page.write_text(text + "\n", encoding="utf-8")
    return {"labels": labels, "counts": counts, "page": page}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write", action="store_true",
        help="Write rebuilt/DATE.json. Frozen-copy mornings 2026-10-13..2026-11-04 are replaced from theme-radar raw files. Other existing files stay.",
    )
    parser.add_argument(
        "--counts-only", action="store_true",
        help="Classify sealed both-halves-0 days and refresh the label index.",
    )
    args = parser.parse_args(argv)
    docs = None
    if not args.counts_only:
        result = run_rebuild(write=args.write)
        docs = result.get("docs")
        print(
            f"[factor-mine-rebuild] dates={len(result.get('dates') or [])} "
            f"wrote={len(result.get('wrote') or [])} "
            f"already={len(result.get('skipped') or [])}",
            flush=True,
        )
    report = classify_books(docs=docs)
    paths = write_labels(report)
    print(counts_table(report), flush=True)
    print(f"[factor-mine-rebuild] labels {paths['page']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
