"""D's close and volume do not change proxy-panel membership."""
from __future__ import annotations

import copy
import unittest

from research.longhist.membership import proxy_members

SESSION = "2024-06-03"


def _prior(n: int = 25) -> list[dict]:
    """Twenty-five sessions before SESSION. The last one is a volume spike.

    Dates are 2024-04-01 through 2024-04-25, all strictly before SESSION.
    """
    rows = []
    for i in range(n):
        volume = 400_000.0 if i == n - 1 else 100_000.0
        rows.append({
            "date": f"2024-04-{i + 1:02d}",
            "open": 10.0,
            "high": 10.4,
            "low": 9.6,
            "close": 10.0,
            "volume": volume,
        })
    return rows


def _today(**overrides) -> dict:
    bar = {
        "date": SESSION,
        "open": 11.0,   # 10% gap versus prior close 10
        "high": 12.0,
        "low": 10.5,
        "close": 11.2,
        "volume": 150_000.0,
    }
    bar.update(overrides)
    return bar


def _book() -> dict[str, list[dict]]:
    keep = _prior() + [_today()]
    # Same prior tape, but the open gap is only 1%, so it stays out.
    skip = _prior() + [_today(open=10.1)]
    return {"KEEP": keep, "SKIP": skip}


class MembershipIgnoresTodayCloseAndVolume(unittest.TestCase):
    def test_changing_session_close_and_volume_keeps_the_same_names(self) -> None:
        original = _book()
        before = proxy_members(original, SESSION)
        self.assertEqual(before, ["KEEP"])

        mutated = copy.deepcopy(original)
        for bars in mutated.values():
            today = bars[-1]
            self.assertEqual(today["date"], SESSION)
            # A close outside $1–$30, and a volume large enough that folding
            # this bar into the 20-day dollar-volume mean would clear $40M.
            today["close"] = 500.0
            today["volume"] = 100_000_000.0
            today["high"] = 510.0
            today["low"] = 1.0
        after = proxy_members(mutated, SESSION)
        self.assertEqual(after, before)

    def test_prior_close_still_gates_membership(self) -> None:
        """The price filter reads the last bar before D, so this one moves."""
        bars = _book()
        prior = [bar for bar in bars["KEEP"] if bar["date"] < SESSION]
        prior[-1]["close"] = 80.0
        self.assertEqual(proxy_members(bars, SESSION), [])


if __name__ == "__main__":
    unittest.main()
