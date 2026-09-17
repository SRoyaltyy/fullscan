"""Conservative shared-book financing, exposure and borrow accounting.

These are explicit research constraints, not a claim to reproduce a broker's
house margin. Default: gross <= 100%, short <= 50%, no short-proceeds reuse.
Locates must be supplied for strict research; assumptions are flagged otherwise.
"""
from datetime import date as Date
import math

DEFAULTS = {"max_gross": 1.0, "max_long": 1.0, "max_short": .5,
            "short_margin": .5, "require_locate": True,
            "borrow_annual": .01, "borrow": {}}


def policy(override=None):
    out = {**DEFAULTS, **(override or {})}
    for key in ("max_gross", "max_long", "max_short", "short_margin", "borrow_annual"):
        if not math.isfinite(float(out[key])) or float(out[key]) < 0:
            raise ValueError(f"invalid risk limit {key}")
    if out["short_margin"] <= 0:
        raise ValueError("short_margin must be positive")
    return out


def exposure(cash, positions, price, default_side="long"):
    long = short = 0.0
    for t, lot in positions.items():
        value = lot["shares"] * price(t, lot)
        if lot.get("side", default_side) == "short":
            short += value
        else:
            long += value
    equity = cash + long - short
    return {"long_value": long, "short_value": short, "gross_value": long + short,
            "equity": equity, "gross_ratio": (long + short) / equity if equity > 0 else None}


def room(cash, positions, price, side, risk, default_side="long"):
    e = exposure(cash, positions, price, default_side)
    eq, long, short = e["equity"], e["long_value"], e["short_value"]
    if eq <= 0:
        return 0.0
    free = cash - (1 + risk["short_margin"]) * short
    gross = eq * risk["max_gross"] - long - short
    if side == "long":
        return max(0., min(free, gross, eq * risk["max_long"] - long))
    return max(0., min(free / risk["short_margin"], gross, eq * risk["max_short"] - short))


def locate(risk, date, ticker):
    row = risk.get("borrow", {}).get(date, {}).get(ticker)
    if row is None:
        return (not risk["require_locate"], risk["borrow_annual"], "assumed borrow")
    from .research_validation import timestamp
    from zoneinfo import ZoneInfo
    from datetime import datetime
    try:
        rate = float(row["annual_rate"])
        deadline = datetime.fromisoformat(date + "T09:30:00").replace(tzinfo=ZoneInfo("America/New_York"))
        seen = timestamp(row["observed_at"])
        ok = row.get("available") is True and seen <= deadline and seen.astimezone(ZoneInfo("America/New_York")).date().isoformat() == date
        return (ok and math.isfinite(rate) and rate >= 0, rate, "dated locate")
    except (KeyError, ValueError, TypeError):
        return False, 0., "invalid locate"


def accrue(cash, positions, date, price, trades, default_side="long"):
    """Accrue calendar days, including weekends, before any cover/partial cover."""
    for ticker, lot in positions.items():
        if lot.get("side", default_side) != "short":
            continue
        prior = lot.get("borrow_through", lot["entry_date"])
        days = (Date.fromisoformat(date) - Date.fromisoformat(prior)).days
        if days <= 0:
            continue
        fee = lot["shares"] * price(ticker, lot) * lot.get("borrow_annual", .01) * days / 365
        cash -= fee
        lot["borrow_through"] = date
        lot["fee_in"] = lot.get("fee_in", 0) + fee
        lot["cost"] = lot.get("cost", 0) + fee
        trades.append({"date": date, "ticker": ticker, "side": "BORROW", "shares": 0,
                       "price": None, "fees": fee, "pnl": None, "cash_after": round(cash, 2),
                       "reason": f"borrow accrual {days} calendar days"})
    return cash


def affordable(shares, price, budget, fee_fn):
    lo, hi = 0, max(0, int(shares))
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if mid * price + fee_fn(mid) <= budget + 1e-9:
            lo = mid
        else:
            hi = mid - 1
    return lo
