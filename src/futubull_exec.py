"""Push flatten_hard_red live tickets into a Futubull / moomoo account.

This cloud box cannot see your Futubull app. OpenD has to be running on
a machine you are logged into (desktop or the ECS box). Then:

    python -m src.futubull_exec --date 2026-09-04          # dry-run
    python -m src.futubull_exec --date 2026-09-04 --submit  # paper (SIMULATE)
    python -m src.futubull_exec --submit --live             # REAL — needs FUTU_LIVE=1

Rules the sender will not break:
  * only live card tickets (never the would-buy wish list)
  * re-plan against the account's real cash + positions
  * skip a name already held; skip if leftover cash cannot size a share
  * hard-red / flatten gates stay the ones on the card
  * REAL orders require --live AND env FUTU_LIVE=1 AND unlock

Env: FUTU_OPEND_HOST (default 127.0.0.1), FUTU_OPEND_PORT (11111),
     FUTU_UNLOCK (trade PIN, real only), FUTU_ACC_ID (optional).
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

from src.sleeve_merge import OUT_DIR
from src.sleeve_merge_live import (
    TODAY_JSON,
    inject_today_from_disk,
    plan_today,
    replay_open,
    write_card,
)

LAST_JSON = OUT_DIR / "futubull_last.json"

US = "US"


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _code(ticker: str) -> str:
    t = str(ticker or "").upper().strip()
    if "." in t and t.split(".", 1)[0] in ("US", "HK", "SG"):
        return t
    return f"{US}.{t}"


def _ticker(code: str) -> str:
    c = str(code or "").upper()
    return c.split(".", 1)[-1] if "." in c else c


class BrokerSnap:
    def __init__(self, *, env: str, cash: float, positions: dict,
                 acc_id: int | None = None, connected: bool = True,
                 error: str | None = None, buying_power: float | None = None):
        self.env = env
        self.cash = float(cash)
        self.buying_power = float(buying_power if buying_power is not None else cash)
        self.positions = {str(k).upper(): dict(v) for k, v in (positions or {}).items()}
        self.acc_id = acc_id
        self.connected = connected
        self.error = error

    @property
    def mv(self) -> float:
        return sum(float(p.get("mv") or 0) for p in self.positions.values())


def sim_from_broker(snap: BrokerSnap, replay: dict | None = None) -> dict:
    """Use the account as the open book. Sleeve unknown → treat as .io holds."""
    open_io = {}
    for t, p in snap.positions.items():
        sh = int(p.get("shares") or 0)
        if sh < 1:
            continue
        px = float(p.get("cost_px") or p.get("last_px") or 0) or 1.0
        last = float(p.get("last_px") or px)
        open_io[t] = {
            "ticker": t, "shares": sh, "side": "BUY",
            "entry_px": px, "sleeve": "io_core",
            "entry_date": str(p.get("entry_date") or "broker"),
            "last_px": last, "fee_in": 0.0,
            "notional": round(sh * px, 2),
        }
    return {
        "calendar": list((replay or {}).get("calendar") or []),
        "cash": snap.cash,
        "open_io": open_io,
        "open_mover": [],
        "trades": [],
        "skipped": [],
        "curve": [],
    }


def plan_for_broker(date: str, snap: BrokerSnap,
                    replay: dict | None = None) -> dict:
    replay = replay if replay is not None else replay_open(date)
    return plan_today(date, capital=max(snap.cash, 1.0),
                      sim=sim_from_broker(snap, replay))


def tickets_to_send(card: dict) -> list[dict]:
    """Live BUY/SELL only. Would-buy is never an order."""
    out = []
    for t in card.get("tickets") or []:
        if t.get("status") == "skip":
            continue
        if t.get("side") not in ("BUY", "SELL"):
            continue
        if int(t.get("shares") or 0) < 1:
            continue
        if not t.get("ticker"):
            continue
        out.append(dict(t))
    return out


def refuse_real(env: str, submit: bool, live_flag: bool) -> str | None:
    if env != "real":
        return None
    if not submit:
        return None
    if not live_flag:
        return "REAL submit refused — pass --live"
    if _env("FUTU_LIVE") != "1":
        return "REAL submit refused — set FUTU_LIVE=1"
    return None


class OpenD:
    """Thin futu-api wrapper. Missing package / dead host → connected=False."""

    def __init__(self, host: str | None = None, port: int | None = None):
        self.host = host or _env("FUTU_OPEND_HOST", "127.0.0.1")
        self.port = int(port or _env("FUTU_OPEND_PORT", "11111") or 11111)
        self.trd = None
        self.err: str | None = None

    def connect(self, env: str) -> bool:
        try:
            from futu import OpenSecTradeContext, TrdMarket, SecurityFirm
        except ImportError:
            self.err = "futu-api not installed (pip install futu-api)"
            return False
        try:
            self.trd = OpenSecTradeContext(
                filter_trdmarket=TrdMarket.US,
                host=self.host, port=self.port,
                security_firm=SecurityFirm.FUTUSECURITIES,
            )
        except Exception as e:  # noqa: BLE001 — OpenD down is a soft miss
            self.err = f"OpenD connect failed: {e}"
            return False
        return True

    def close(self) -> None:
        if self.trd is not None:
            try:
                self.trd.close()
            except Exception:
                pass
        self.trd = None

    def unlock(self) -> bool:
        pwd = _env("FUTU_UNLOCK")
        if not pwd or self.trd is None:
            return True
        ret, data = self.trd.unlock_trade(pwd)
        if ret != 0:
            self.err = f"unlock failed: {data}"
            return False
        return True

    def snapshot(self, env: str) -> BrokerSnap:
        if self.trd is None:
            return BrokerSnap(env=env, cash=0, positions={},
                              connected=False, error=self.err or "not connected")
        try:
            from futu import TrdEnv
            trd_env = TrdEnv.REAL if env == "real" else TrdEnv.SIMULATE
        except Exception as e:
            return BrokerSnap(env=env, cash=0, positions={},
                              connected=False, error=str(e))
        acc_kw = {}
        acc_id = _env("FUTU_ACC_ID")
        if acc_id:
            try:
                acc_kw["acc_id"] = int(acc_id)
            except ValueError:
                pass
        ret, info = self.trd.accinfo_query(trd_env=trd_env, **acc_kw)
        if ret != 0:
            return BrokerSnap(env=env, cash=0, positions={},
                              connected=False, error=f"accinfo: {info}")
        row = info.iloc[0].to_dict() if hasattr(info, "iloc") else dict(info)
        cash = float(row.get("cash") or row.get("avl_withdrawal_cash") or 0)
        power = float(row.get("power") or row.get("max_power_short") or cash)
        ret, pos = self.trd.position_list_query(trd_env=trd_env, **acc_kw)
        positions = {}
        if ret == 0 and pos is not None and getattr(pos, "empty", True) is False:
            for _, r in pos.iterrows():
                t = _ticker(str(r.get("code") or ""))
                sh = int(float(r.get("qty") or r.get("can_sell_qty") or 0))
                if not t or sh < 1:
                    continue
                positions[t] = {
                    "shares": sh,
                    "cost_px": float(r.get("cost_price") or 0),
                    "last_px": float(r.get("nominal_price") or r.get("cost_price") or 0),
                    "mv": float(r.get("market_val") or 0),
                }
        return BrokerSnap(env=env, cash=cash, buying_power=power,
                          positions=positions, connected=True,
                          acc_id=acc_kw.get("acc_id"))

    def place(self, ticket: dict, env: str) -> dict:
        if self.trd is None:
            return {"ok": False, "error": self.err or "not connected"}
        from futu import TrdEnv, TrdSide, OrderType
        trd_env = TrdEnv.REAL if env == "real" else TrdEnv.SIMULATE
        side = TrdSide.BUY if ticket["side"] == "BUY" else TrdSide.SELL
        ret, data = self.trd.place_order(
            price=float(ticket["px"]),
            qty=int(ticket["shares"]),
            code=_code(ticket["ticker"]),
            trd_side=side,
            order_type=OrderType.NORMAL,
            trd_env=trd_env,
            remark="flatten_hard_red",
        )
        if ret != 0:
            return {"ok": False, "error": str(data)}
        oid = ""
        if hasattr(data, "iloc"):
            oid = str(data.iloc[0].get("order_id") or "")
        return {"ok": True, "order_id": oid}


def send_card(card: dict, snap: BrokerSnap, *, submit: bool,
              opend: OpenD | None, env: str) -> dict:
    tickets = tickets_to_send(card)
    sent = []
    for t in tickets:
        rec = {
            "ticker": t["ticker"], "side": t["side"],
            "shares": t["shares"], "px": t.get("px"),
            "clock": t.get("clock"), "sleeve": t.get("sleeve"),
        }
        if not submit:
            rec["status"] = "dry_run"
            sent.append(rec)
            continue
        if opend is None:
            rec["status"] = "error"
            rec["error"] = "no OpenD"
            sent.append(rec)
            continue
        got = opend.place(t, env)
        rec.update(got)
        rec["status"] = "submitted" if got.get("ok") else "error"
        sent.append(rec)
    return {
        "date": card.get("date"),
        "env": env,
        "submit": submit,
        "connected": snap.connected,
        "error": snap.error,
        "cash": snap.cash,
        "n_positions": len(snap.positions),
        "n_tickets": len(tickets),
        "n_would": len((card.get("would_buy") or {}).get("rows") or []),
        "sent": sent,
        "generated": datetime.now().isoformat(timespec="seconds"),
    }


def write_last(doc: dict) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    LAST_JSON.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return LAST_JSON


def run(date: str | None, *, env: str = "simulate", submit: bool = False,
        live: bool = False, write: bool = True) -> int:
    env = "real" if env == "real" else "simulate"
    blocked = refuse_real(env, submit, live)
    if blocked:
        print(f"[futubull] {blocked}")
        write_last({"error": blocked, "env": env, "submit": submit,
                    "generated": datetime.now().isoformat(timespec="seconds")})
        return 2

    from src.sleeve_merge_live import et_today
    date = date or et_today()
    opend = OpenD()
    snap: BrokerSnap
    if opend.connect(env):
        if env == "real" and submit and not opend.unlock():
            snap = BrokerSnap(env=env, cash=0, positions={},
                              connected=False, error=opend.err)
        else:
            snap = opend.snapshot(env)
    else:
        snap = BrokerSnap(env=env, cash=0, positions={},
                          connected=False, error=opend.err)

    if not snap.connected:
        print(f"[futubull] not connected — {snap.error}")
        print("[futubull] OpenD must be running on a machine you are logged "
              "into. This cloud job cannot open Futubull for you.")
        replay = replay_open(date)
        card = plan_today(date, sim=replay)
        last = {
            "date": date, "env": env, "submit": False,
            "connected": False, "error": snap.error,
            "n_tickets": len(tickets_to_send(card)),
            "sent": [],
            "generated": datetime.now().isoformat(timespec="seconds"),
        }
        if write:
            write_last(last)
            if TODAY_JSON.is_file():
                inject_today_from_disk()
        opend.close()
        return 0

    card = plan_for_broker(date, snap)
    last = send_card(card, snap, submit=submit, opend=opend, env=env)
    print(f"[futubull] {env} connected cash=${snap.cash:,.2f} "
          f"pos={len(snap.positions)} tickets={last['n_tickets']} "
          f"submit={submit}")
    for s in last["sent"]:
        print(f"  {s.get('status')} {s['side']} {s['ticker']} "
              f"n={s['shares']} @ {s.get('px')} {s.get('error') or ''}")
    if write:
        write_card(card)
        write_last(last)
        inject_today_from_disk()
    opend.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument("--env", choices=("simulate", "real"), default="simulate")
    ap.add_argument("--submit", action="store_true",
                    help="place orders (default is dry-run)")
    ap.add_argument("--live", action="store_true",
                    help="required together with --env real and FUTU_LIVE=1")
    ap.add_argument("--write", action="store_true", default=True)
    args = ap.parse_args(argv)
    return run(args.date or None, env=args.env, submit=args.submit,
               live=args.live, write=args.write)


if __name__ == "__main__":
    raise SystemExit(main())
