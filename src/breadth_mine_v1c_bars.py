"""IRONCLAD rule 26 for breadth_mine_v1c.

A consecutive-session jump is this session's open divided by the
previous session's close. The CI test and the scorer both refuse
a jump above 3x or below 1/3 unless a Yahoo split on the later
session's date matches the ratio.
"""
from __future__ import annotations

from src.breadth_mine_v1b_bars import load_bars, load_splits, split_explains

RATIO_HI = 3.0
RATIO_LO = 1.0 / 3.0


def session_jumps(bars: dict[str, list[dict]] | None = None,
                  splits: dict[tuple[str, str], float] | None = None) -> list[dict]:
    if bars is None:
        bars = load_bars()
    if splits is None:
        splits = load_splits()
    flags = []
    for ticker in sorted(bars):
        prev = None
        for row in bars[ticker]:
            if prev is not None and prev["close"] > 0 and row["open"] > 0:
                ratio = row["open"] / prev["close"]
                if ratio > RATIO_HI or ratio < RATIO_LO:
                    split = splits.get((ticker, row["date"]))
                    explained = split is not None and split_explains(ratio, split)
                    flags.append({
                        "date": row["date"],
                        "explained": explained,
                        "ratio": round(float(ratio), 6),
                        "split": None if split is None else round(float(split), 8),
                        "ticker": ticker,
                    })
            prev = row
    return flags


def unexplained(flags: list[dict] | None = None) -> list[dict]:
    if flags is None:
        flags = session_jumps()
    return [row for row in flags if not row["explained"]]


def assert_split_consistent() -> None:
    bad = unexplained()
    if bad:
        sample = ", ".join(f"{row['ticker']} {row['date']}" for row in bad[:8])
        raise SystemExit(
            f"IRONCLAD 26: {len(bad)} unexplained consecutive-session jumps, including {sample}"
        )


if __name__ == "__main__":
    assert_split_consistent()
