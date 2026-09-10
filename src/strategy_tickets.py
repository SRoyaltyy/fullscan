"""Morning BUY/SELL for every strategy the dashboards already list.

Stock-book horizons and flatten come off files that Pre-Open / Stock Book
already wrote. Factor-mine recipes use pick_day on the leak-free panel
when that date exists. Nothing here fetches prices. Soft-fail every
source so publish_live_boards still writes a strip.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo("America/New_York")
BOOK = ROOT / "data" / "stock_book"
DAY = ROOT / "data" / "day_board"
FM_DIR = ROOT / "data" / "factor_mine"
DASH_FM = ROOT / "dashboard" / "factor-mine"
SLEEVE = ROOT / "data" / "sleeve_merge" / "today.json"
PANEL = FM_DIR / "panel.json"
SCORE_FM = ROOT / "03_scoreboard" / "factor_mine"


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _tickers(rows) -> list[dict]:
    out = []
    for row in rows or []:
        if isinstance(row, dict):
            t = str(row.get("ticker") or row.get("symbol") or "").strip().upper()
            if not t:
                continue
            item = {"ticker": t}
            if row.get("score") is not None:
                item["score"] = row.get("score")
            elif row.get("total") is not None:
                item["score"] = row.get("total")
            out.append(item)
        else:
            t = str(row).strip().upper()
            if t:
                out.append({"ticker": t})
    return out


def _names(rows) -> list[str]:
    return [x["ticker"] for x in _tickers(rows) if x.get("ticker")]


def _entry(name: str, family: str, date: str, buy, sell, **extra) -> dict:
    buys = _tickers(buy)
    sells = _tickers(sell)
    row = {
        "name": name,
        "family": family,
        "date": date,
        "buy": buys,
        "sell": sells,
        "buy_n": len(buys),
        "sell_n": len(sells),
        "sit": extra.get("sit", False),
        "status": extra.get("status") or "ok",
    }
    for k in ("why", "hard_red", "s", "note"):
        if extra.get(k) is not None:
            row[k] = extra[k]
    return row


def _load_json(path: Path):
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        print(f"[strategy-tickets] WARN: {path.name}: {e}", flush=True)
        return None


def stock_book_strats(date: str) -> list[dict]:
    sug = _load_json(BOOK / f"{date}_suggestions.json") or _load_json(
        BOOK / "latest_suggestions.json"
    )
    book = _load_json(BOOK / f"{date}_stock_book.json")
    if sug is None and book is not None:
        try:
            from . import book_suggestions as bs
            sug = bs.suggestions_from_book({**book, "date": date})
        except Exception as e:  # noqa: BLE001
            print(f"[strategy-tickets] WARN: book suggestions: {e}", flush=True)
    out: list[dict] = []
    horizons = (sug or {}).get("horizons") or {}
    if not horizons and sug:
        horizons = {
            "1d": {
                "buy": [{"ticker": t} for t in sug.get("buy_1d") or []],
                "sell": [{"ticker": t} for t in sug.get("sell_1d") or []],
            }
        }
    for h in ("1d", "3d", "1w", "2w", "1m"):
        block = horizons.get(h) or {}
        out.append(_entry(
            f"stock_book_{h}", "stock_book", date,
            block.get("buy"), block.get("sell"),
            note="weighted ranker horizon",
        ))
        out.append(_entry(
            f"{h}_top", "paper", date,
            block.get("buy"), block.get("sell"),
            note="paper sleeve = same ranker list",
        ))
    return out


def flatten_strat(date: str) -> dict:
    card = _load_json(SLEEVE) or {}
    buys, sells = [], []
    for t in card.get("tickets") or card.get("buys") or []:
        if isinstance(t, dict) and str(t.get("side") or "").upper() in ("SELL", "COVER"):
            sells.append(t)
        else:
            buys.append(t)
    for key in ("buy", "buys_0930", "priced_buys"):
        if card.get(key):
            buys = card.get(key)
            break
    for key in ("sell", "sells_0930"):
        if card.get(key):
            sells = card.get(key)
            break
    sit = bool(card.get("hard_red")) or not bool(card.get("flatten_ok"))
    why = card.get("said") or card.get("route")
    if sit and not buys:
        why = why or "flatten sit — no priced mover BUYs"
    return _entry(
        "flatten_robust", "flatten",
        str(card.get("date") or date),
        buys, sells,
        sit=sit,
        hard_red=bool(card.get("hard_red")),
        s=card.get("score"),
        why=why,
        status="ok" if card else "missing",
        note="live flatten_robust card",
    )


def recipe_strats(date: str) -> list[dict]:
    try:
        from . import factor_mine as fm
        from . import factor_mine_book as fmb
    except Exception as e:  # noqa: BLE001
        print(f"[strategy-tickets] WARN: factor-mine import: {e}", flush=True)
        return []
    raw = _load_json(PANEL)
    if not raw:
        return [_entry("factor_mine", "factor_mine", date, [], [],
                       status="no_panel", note="panel.json missing")]
    panel = fm.rehydrate_panel(raw)
    by_date = panel.get("by_date") or {}
    use_date = date if date in by_date else None
    if use_date is None:
        dates = list(panel.get("session_dates") or [])
        older = [d for d in dates if d <= date]
        use_date = older[-1] if older else (dates[-1] if dates else None)
    rows = by_date.get(use_date) or []
    s = None
    try:
        s = fmb.morning_s(fmb.load_regime(), date)
    except Exception:
        try:
            s = fmb.morning_s(fmb.load_regime(), use_date or date)
        except Exception:
            s = None
    hard = s is not None and float(s) <= -3.0
    recs = list(fm.build_recipes())
    have = {r["name"] for r in recs}
    if SCORE_FM.is_dir():
        for p in SCORE_FM.glob("*.md"):
            name = p.stem
            if name not in have:
                recs.append(fm.make_recipe(name=name, note="scoreboard blotter"))
                have.add(name)
    out = []
    stale = use_date != date
    for rec in recs:
        name = rec["name"]
        if rec.get("universe") == "combo" or rec.get("members") or name.startswith("combo_"):
            out.append(_entry(
                name, "factor_mine", use_date or date, [], [],
                status="combo_needs_mine",
                note="combo pile needs the cash-book roll",
            ))
            continue
        if not rows:
            out.append(_entry(
                name, "factor_mine", date, [], [],
                status="no_panel_day",
                note=f"no panel rows for {date}",
            ))
            continue
        try:
            picked = fm.pick_day(rows, rec)
        except Exception as e:  # noqa: BLE001
            out.append(_entry(
                name, "factor_mine", use_date or date, [], [],
                status="pick_fail", note=str(e)[:160],
            ))
            continue
        buys = [{"ticker": r["ticker"], "src": ",".join(r.get("sources") or [])}
                for r in picked if r.get("ticker")]
        if hard:
            out.append(_entry(
                name, "factor_mine", use_date or date, [], [],
                sit=True, hard_red=True, s=s,
                status="sit",
                note="hard-red S\u2264\u22123 — no new lots",
                why=f"S={s}",
            ))
        else:
            out.append(_entry(
                name, "factor_mine", use_date or date, buys, [],
                s=s,
                status="stale_panel" if stale else "ok",
                note=("would-buy at 09:30; sells need cash-book lots"
                      + (f" · panel {use_date}" if stale else "")),
            ))
    return out


def build(date: str) -> dict:
    strats: list[dict] = []
    errors: list[str] = []
    try:
        strats.extend(stock_book_strats(date))
    except Exception as e:  # noqa: BLE001
        errors.append(f"stock_book:{e}")
    try:
        strats.append(flatten_strat(date))
    except Exception as e:  # noqa: BLE001
        errors.append(f"flatten:{e}")
        strats.append(_entry("flatten_robust", "flatten", date, [], [],
                             status="error", note=str(e)[:160]))
    try:
        strats.extend(recipe_strats(date))
    except Exception as e:  # noqa: BLE001
        errors.append(f"recipes:{e}")
    by_name = {s["name"]: s for s in strats}
    ok = sum(1 for s in strats if s.get("status") in ("ok", "sit", "stale_panel"))
    return {
        "date": date,
        "generated_at": datetime.now(ET).isoformat(),
        "n": len(strats),
        "n_ok": ok,
        "families": sorted({s.get("family") for s in strats}),
        "strategies": by_name,
        "order": [s["name"] for s in strats],
        "errors": errors,
        "source": "strategy_tickets",
    }


def write(date: str, payload: dict | None = None) -> list[Path]:
    payload = payload or build(date)
    text = json.dumps(payload, indent=2)
    paths = [
        DAY / "strategy_tickets.json",
        DAY / f"{date}_strategy_tickets.json",
        FM_DIR / "strategy_tickets.json",
        DASH_FM / "strategy_tickets.json",
        DASH_FM / "today_strategies.json",
    ]
    wrote = []
    for p in paths:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
        wrote.append(p)
    slim = {
        "date": payload.get("date"),
        "generated_at": payload.get("generated_at"),
        "n": payload.get("n"),
        "n_ok": payload.get("n_ok"),
        "buy_1d": _names((payload.get("strategies") or {}).get("stock_book_1d", {}).get("buy")),
        "sell_1d": _names((payload.get("strategies") or {}).get("stock_book_1d", {}).get("sell")),
        "strategies": {
            k: {
                "buy": _names(v.get("buy")),
                "sell": _names(v.get("sell")),
                "sit": v.get("sit"),
                "status": v.get("status"),
                "family": v.get("family"),
                "date": v.get("date"),
            }
            for k, v in (payload.get("strategies") or {}).items()
        },
    }
    slim_path = DAY / "today_strategies.json"
    slim_path.write_text(json.dumps(slim, indent=2), encoding="utf-8")
    wrote.append(slim_path)
    print(
        f"[strategy-tickets] {date} n={payload.get('n')} ok={payload.get('n_ok')} "
        f"errors={payload.get('errors') or []}",
        flush=True,
    )
    return wrote


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args(argv)
    date = args.date or _today()
    payload = build(date)
    if args.write:
        write(date, payload)
    else:
        print(json.dumps({
            "date": payload["date"], "n": payload["n"],
            "n_ok": payload["n_ok"], "errors": payload["errors"],
            "sample": payload["order"][:12],
        }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
