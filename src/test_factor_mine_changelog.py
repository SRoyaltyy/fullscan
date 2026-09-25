"""Changelog parser, nightly filter, and past-day rewrite rules."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from . import factor_mine_changelog as log


BLOT = """# Factor mine action — `toy`

Cash book **+10.00%** ($11,000) · signal-only was +1.00%.

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px |
|---|---|---|---:|---:|
| 2026-08-13 09:30 ET | **BUY** | `AAA` | 10 | $5.00 |
| 2026-08-13 09:30 ET | **BUY** | `BBB` | 1,152 | $1.00 |
| 2026-08-14 09:30 ET | **SELL** | `AAA` | 10 | $6.00 |
| 2026-08-14 09:30 ET | **SHORT** | `EU` | 3 | $2.00 |
| 2026-08-14 09:30 ET | **COVER** | `EU` | 3 | $1.50 |
"""


def test_parse_blotter_orders_buys_and_sells() -> None:
    book = log.parse_blotter(BLOT)
    assert book["total_pct"] == 10.0
    assert book["equity"] == "11000"
    day = book["days"]["2026-08-13"]
    assert day["buys"] == ["BUY AAA×10", "BUY BBB×1152"]
    assert day["sells"] == []
    nxt = book["days"]["2026-08-14"]
    assert nxt["sells"] == ["SELL AAA×10", "SHORT EU×3"]
    assert nxt["buys"] == ["COVER EU×3"]


def test_past_day_change_is_a_rewrite_and_a_new_day_is_not() -> None:
    prev = {
        "toy": log.parse_blotter(BLOT),
    }
    curr_text = BLOT.replace("**BUY** | `AAA` | 10", "**BUY** | `AAA` | 12")
    curr_text = curr_text.replace(
        "Cash book **+10.00%**", "Cash book **+12.50%**",
    )
    curr_text += (
        "\n| 2026-08-17 09:30 ET | **BUY** | `CCC` | 4 | $8.00 |\n"
    )
    curr = {"toy": log.parse_blotter(curr_text)}
    diff = log.diff_books(prev, curr)
    assert diff["recipes_checked"] == 1
    assert len(diff["changes"]) == 1
    row = diff["changes"][0]
    assert row["recipe"] == "toy"
    assert row["delta"] == 2.5
    assert [day["date"] for day in row["days"]] == ["2026-08-13"]
    assert "AAA×10" in row["days"][0]["buys"]
    assert "AAA×12" in row["days"][0]["buys"]
    assert row["appended"] == ["2026-08-17"]
    assert "2026-08-17" not in [day["date"] for day in row["days"]]


def test_unchanged_tickers_are_not_a_rewrite() -> None:
    book = log.parse_blotter(BLOT)
    later = log.parse_blotter(BLOT.replace("+10.00%", "+11.00%"))
    diff = log.diff_books({"toy": book}, {"toy": later})
    assert diff["changes"] == []
    assert diff["moves"][0]["delta"] == 1.0
    assert diff["moves"][0]["rewritten_days"] == 0


def test_nightly_commits_keep_the_window_and_the_chore_subject() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        repo = Path(tmp)
        blotter = repo / "03_scoreboard" / "factor_mine"
        blotter.mkdir(parents=True)
        env = os.environ.copy()
        env.update({
            "GIT_AUTHOR_NAME": "test",
            "GIT_AUTHOR_EMAIL": "test@example.com",
            "GIT_COMMITTER_NAME": "test",
            "GIT_COMMITTER_EMAIL": "test@example.com",
        })

        def commit(name: str, when: str, subject: str) -> None:
            path = blotter / name
            path.write_text(BLOT, encoding="utf-8")
            subprocess.run(["git", "add", str(path)], cwd=repo, check=True, env=env)
            env["GIT_AUTHOR_DATE"] = when
            env["GIT_COMMITTER_DATE"] = when
            subprocess.run(
                ["git", "commit", "-m", subject],
                cwd=repo, check=True, env=env,
            )

        subprocess.run(["git", "init"], cwd=repo, check=True, env=env)
        commit("early.md", "2026-07-01T21:00:00+00:00", log.NIGHTLY_SUBJECT)
        commit("mid.md", "2026-09-10T21:00:00+00:00", log.NIGHTLY_SUBJECT + " 2026-08-13")
        commit("other.md", "2026-09-11T21:00:00+00:00", "docs: not a nightly mine")
        commit("late.md", "2026-09-24T21:44:00+00:00", log.NIGHTLY_SUBJECT)
        nights = log.nightly_commits(repo)
        assert [night["et_date"] for night in nights] == ["2026-09-10", "2026-09-24"]
        books = log.load_books(repo, nights[0]["sha"])
        assert "mid" in books
        assert books["mid"]["total_pct"] == 10.0
        assert books["mid"]["days"]["2026-08-13"]["buys"][0] == "BUY AAA×10"


def test_summary_counts_rewrite_nights_and_renders() -> None:
    nights = [
        {"sha": "a" * 40, "short": "aaaaaaaaa", "committed_at": "2026-09-10 17:00 ET",
         "et_date": "2026-09-10", "subject": log.NIGHTLY_SUBJECT, "recipes": 1},
        {"sha": "b" * 40, "short": "bbbbbbbbb", "committed_at": "2026-09-11 17:00 ET",
         "et_date": "2026-09-11", "subject": log.NIGHTLY_SUBJECT, "recipes": 1},
    ]
    diffs = [{
        "prev": nights[0],
        "curr": nights[1],
        "recipes_checked": 1,
        "prev_recipes": 1,
        "curr_recipes": 1,
        "added": [],
        "removed": [],
        "changes": [{
            "recipe": "toy",
            "total_before": 10.0,
            "total_after": 12.5,
            "delta": 2.5,
            "days": [{
                "date": "2026-08-13",
                "buys": "BUY AAA×10 -> BUY AAA×12",
                "sells": "same",
            }],
            "appended": [],
        }],
        "moves": [{
            "recipe": "toy",
            "total_before": 10.0,
            "total_after": 12.5,
            "delta": 2.5,
            "rewritten_days": 1,
        }],
    }]
    stats = log.summary_stats(nights, diffs)
    assert stats["rewrite_nights"] == 1
    assert stats["recipes_affected"] == 1
    assert stats["day_rows"] == 1
    text = log.render_markdown(nights, diffs, {
        "status": "PASS",
        "detail": "Two rebuilds matched.",
        "lines": ["Frozen ledgers were left in place."],
    })
    assert "**PASS.**" in text
    assert "BUY AAA×10 -> BUY AAA×12" in text
    assert "rules pull request" in text
    assert "toy" in text


if __name__ == "__main__":
    test_parse_blotter_orders_buys_and_sells()
    test_past_day_change_is_a_rewrite_and_a_new_day_is_not()
    test_unchanged_tickers_are_not_a_rewrite()
    test_nightly_commits_keep_the_window_and_the_chore_subject()
    test_summary_counts_rewrite_nights_and_renders()
    print("factor-mine changelog tests passed")
