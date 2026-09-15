"""Refresh live dashboard tickets after the book / packet lands.

Cheap path (this module):
  * rewrite data/day_board/*.json so factor-mine + day-board strips
    poll today's 1d BUY/SELL from main
  * write data/stock_book/*_suggestions.json for the paper dashboard
  * copy the same strip to dashboard/factor-mine/today.json (Pages
    same-origin fallback)
  * inject the paper-dash live poller if the baked HTML is missing it

Paper / sleeve / strategy-board HTML rebuilds are extras. Names must
already be on the dashboards from the JSON strip after the book lands.

The 90-minute factor-mine recipe grid is NOT run here. Morning
every-sleeve BUY/SELL is ``strategy_tickets`` (session-open look).
Stock Book ALL and Pre-Open ALL still kick ``factor_mine.yml``
``--land-closed`` after the close so the cash blotter rolls; that
path is a no-op in the morning once yesterday is already on the board.

CLI: python -m src.publish_live_boards --date YYYY-MM-DD --write
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)
BOOK = ROOT / "data" / "stock_book"
DAY_BOARD = ROOT / "data" / "day_board"
FM_TODAY = ROOT / "dashboard" / "factor-mine" / "today.json"


def ticket_1d_rows(payload: dict) -> tuple[list, list, dict]:
    """Session-open 1d names + Elite quote from tickets, not a later re-rank."""
    sb = (payload.get("strategies") or {}).get("stock_book_1d") or {}
    buys = list(payload.get("buy_1d") or sb.get("buy") or [])
    sells = list(payload.get("sell_1d") or sb.get("sell") or [])
    return buys, sells, payload.get("quote") or {}


def rewrite_today_strip(date: str, buys: list, sells: list, quote: dict | None,
                        generated_at: str | None = None) -> list[str]:
    """Keep today.json / factor-mine today.json on the same live 1d book."""
    wrote = []
    paths = (
        DAY_BOARD / "today.json",
        FM_TODAY,
        DAY_BOARD / f"{date}_tickets.json",
    )
    for path in paths:
        prev: dict = {}
        if path.is_file():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError, json.JSONDecodeError):
                raw = {}
            if isinstance(raw, dict):
                prev = raw
        prev["date"] = date
        if generated_at:
            prev["generated_at"] = generated_at
        prev["buy_1d"] = buys
        prev["sell_1d"] = sells
        if quote:
            prev["quote"] = quote
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(prev, indent=2), encoding="utf-8")
        try:
            wrote.append(str(path.relative_to(ROOT)))
        except ValueError:
            wrote.append(str(path))
    return wrote


def load_live_ticket_payload(date: str) -> dict:
    """On-disk 09:30 tickets with Elite live px, or {}."""
    paths = (
        DAY_BOARD / "today_strategies.json",
        DAY_BOARD / f"{date}_strategy_tickets.json",
        ROOT / "dashboard" / "factor-mine" / "strategy_tickets.json",
        ROOT / "dashboard" / "today_strategies.json",
    )
    for path in paths:
        if not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        legal = str(data.get("clock_legal_for") or data.get("date") or "")
        if legal and legal != date:
            continue
        quote = data.get("quote") or {}
        src = str(quote.get("src") or "")
        if not quote.get("after_open") or not src.startswith("elite_live"):
            continue
        buys, sells, _quote = ticket_1d_rows(data)
        if buys or sells:
            return data
    return {}


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _book_ok(date: str) -> bool:
    return (BOOK / f"{date}_stock_book.json").is_file() or (
        ROOT / "01_daily" / f"{date}_stock_book.md"
    ).is_file()


def _run(argv: list[str], timeout_s: int = 180) -> int:
    print("[live-boards] $ " + " ".join(argv), flush=True)
    try:
        r = subprocess.run(argv, cwd=str(ROOT), timeout=timeout_s)
    except subprocess.TimeoutExpired:
        print(f"[live-boards] WARN: timeout {timeout_s}s: {' '.join(argv)}",
              flush=True)
        return 124
    except OSError as e:
        print(f"[live-boards] WARN: {e}", flush=True)
        return 1
    if r.returncode:
        print(f"[live-boards] WARN: exit {r.returncode}: {' '.join(argv)}",
              flush=True)
    return r.returncode


def publish(date: str, *, write: bool = True, extras: bool = True) -> dict:
    """Rewrite day-board + optional live HTML. Soft-fail everything."""
    out: dict = {"date": date, "book_ok": _book_ok(date), "wrote": []}
    if not out["book_ok"]:
        print(f"[live-boards] no stock book on disk for {date} — "
              "strip stays empty until the ranker lands", flush=True)
    try:
        from . import day_board
        board = day_board.build(date)
        live = load_live_ticket_payload(date)
        if live:
            buys, sells, quote = ticket_1d_rows(live)
            sel = board.setdefault("selections", {})
            sel["buy_1d"] = buys
            sel["sell_1d"] = sells
            board["quote"] = quote
            print(
                "[live-boards] keep 09:30 Elite 1d "
                f"buy={[r.get('ticker') for r in buys[:6] if isinstance(r, dict)]}",
                flush=True,
            )
        if write:
            paths = day_board.write_json(board)
            out["wrote"].extend(str(p.relative_to(ROOT)) for p in paths)
            try:
                hp = day_board.write_html()
                out["wrote"].append(str(hp.relative_to(ROOT)))
            except Exception as e:  # noqa: BLE001
                print(f"[live-boards] WARN: day-board html: {e}", flush=True)
        sel = board.get("selections") or {}
        out["buy_1d"] = [r.get("ticker") for r in (sel.get("buy_1d") or [])]
        out["sell_1d"] = [r.get("ticker") for r in (sel.get("sell_1d") or [])]
        out["overall"] = board.get("overall")
        if write:
            FM_TODAY.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "date": date,
                "generated_at": board.get("generated_at"),
                "overall": board.get("overall"),
                "ranker_ready": board.get("ranker_ready"),
                "counts": board.get("counts") or {},
                "buy_1d": sel.get("buy_1d") or [],
                "sell_1d": sel.get("sell_1d") or [],
                "quote": board.get("quote") or {},
                "flatten": sel.get("flatten") or {},
                "general": sel.get("general") or {},
                "sectors": sel.get("sectors") or {},
                "lands": board.get("lands") or [],
                "source": "publish_live_boards",
            }
            FM_TODAY.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            out["wrote"].append("dashboard/factor-mine/today.json")
            dated = DAY_BOARD / f"{date}_tickets.json"
            dated.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            out["wrote"].append(str(dated.relative_to(ROOT)))
    except Exception as e:  # noqa: BLE001
        print(f"[live-boards] WARN: day-board rebuild failed: {e}", flush=True)
        out["error"] = str(e)

    if write:
        try:
            from . import strategy_tickets as st
            payload_st = st.build(date)
            paths = st.write(date, payload_st)
            out["wrote"].extend(str(p.relative_to(ROOT)) for p in paths)
            live = load_live_ticket_payload(date)
            src = live or payload_st
            out["n_strategies"] = src.get("n")
            out["n_strategies_ok"] = src.get("n_ok")
            if live:
                buys, sells, quote = ticket_1d_rows(live)
                synced = rewrite_today_strip(
                    date, buys, sells, quote,
                    generated_at=live.get("generated_at")
                    or payload_st.get("generated_at"),
                )
                out["wrote"].extend(synced)
                out["buy_1d"] = [
                    r.get("ticker") for r in buys if isinstance(r, dict)
                ]
                out["sell_1d"] = [
                    r.get("ticker") for r in sells if isinstance(r, dict)
                ]
                out["quote"] = quote
            else:
                print(
                    "[live-boards] skip today.json rewrite — "
                    "tickets not elite_live after rebuild",
                    flush=True,
                )
        except Exception as e:  # noqa: BLE001
            print(f"[live-boards] WARN: strategy tickets: {e}", flush=True)

    if write:
        try:
            from . import hold_live_px as hlp
            for path in hlp.write(date=date):
                out["wrote"].append(str(path.relative_to(ROOT)))
        except Exception as e:  # noqa: BLE001
            print(f"[live-boards] WARN: hold live px: {e}", flush=True)

    if write:
        try:
            from . import book_suggestions
            sug = book_suggestions.write(date=date)
            if sug is not None:
                out["wrote"].append(str(sug.relative_to(ROOT)))
            out["poller_injected"] = book_suggestions.ensure_dashboard_poller()
            extra = book_suggestions.ensure_live_board_pollers()
            if extra:
                out["wrote"].extend(extra)
                out["poller_injected"] = True
        except Exception as e:  # noqa: BLE001
            print(f"[live-boards] WARN: suggestions sidecar: {e}", flush=True)

    if extras and write:
        py = sys.executable
        _run([py, "-m", "src.sleeve_merge", "--card", "--date", date,
              "--write-card"], timeout_s=120)
        _run([py, "-m", "src.paper_trade", "--date", date, "--top", "10"],
             timeout_s=180)
        _run([py, "-m", "src.strategy_board", "--write"], timeout_s=180)
        _run([py, "-m", "src.sleeve_merge", "--write"], timeout_s=180)

    print(
        f"[live-boards] {date} book_ok={out['book_ok']} "
        f"buy={out.get('buy_1d') or []} sell={out.get('sell_1d') or []} "
        f"wrote={len(out.get('wrote') or [])}",
        flush=True,
    )
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="", help="YYYY-MM-DD (default today ET)")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--no-extras", action="store_true",
                    help="Only rewrite the 1d strip JSON (no paper/sleeve)")
    args = ap.parse_args(argv)
    date = args.date or _today()
    publish(date, write=args.write, extras=not args.no_extras)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
