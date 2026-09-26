"""Write a human-readable daily summary for the Excel-replica bot.

Run AFTER daily_run.py, from the excel_bot/ directory:
    python engine/gh_summary.py

Reads suggestions/suggestions.csv.

The schedule and the after-close dispatch share this writer. Before
16:00 ET it writes daily/{date}_excel_bot_draft.md only and does not
create or modify the final dated file. At or after 16:00 ET it creates
daily/{date}_excel_bot.md once. A second after-close run refuses to
overwrite that file. Sibling .csv/.json dated artifacts use the same
rule. Zero network, zero tokens — pure stdlib + the suggestions file.
"""
import csv
import os
from datetime import datetime
from zoneinfo import ZoneInfo

SUGG = "suggestions/suggestions.csv"
OUT_DIR = "daily"
ET = ZoneInfo("America/New_York")
CLOSE_HOUR = 16
DATED_EXTS = (".md", ".csv", ".json")


class FinalSignalExists(RuntimeError):
    """The after-close dated file is already on disk. It is not rewritten."""

    def __init__(self, path):
        self.path = path
        super().__init__(
            f"[summary] REFUSE: {path} already exists. "
            "After-close final is write-once and was not overwritten."
        )


def session_clock(now=None):
    """America/New_York clock. A naive value is already ET."""
    if now is None:
        return datetime.now(ET)
    if now.tzinfo is None:
        return now.replace(tzinfo=ET)
    return now.astimezone(ET)


def is_after_close(now=None):
    """True at or after 16:00 ET on the session clock's calendar day."""
    clock = session_clock(now)
    close = clock.replace(hour=CLOSE_HOUR, minute=0, second=0, microsecond=0)
    return clock >= close


def dated_name(day, ext, *, draft):
    """`<date>_excel_bot.md` or `<date>_excel_bot_draft.md` (and csv/json)."""
    if not ext.startswith("."):
        ext = "." + ext
    if ext not in DATED_EXTS:
        raise ValueError(f"unsupported dated signal extension: {ext}")
    stem = f"{day}_excel_bot_draft" if draft else f"{day}_excel_bot"
    return stem + ext


def _exclusive_write(path, text):
    """Create `path` only when it is absent. The existing file is not opened."""
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    try:
        os.link(tmp, path)
    except FileExistsError:
        raise FinalSignalExists(path) from None
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


def write_dated(out_dir, day, ext, text, *, after_close):
    """Write the draft or create the final. Never overwrite a final.

    Before the close, the final path is never opened for writing. After
    the close, an existing final raises FinalSignalExists and is left
    byte-identical.
    """
    os.makedirs(out_dir, exist_ok=True)
    final = os.path.join(out_dir, dated_name(day, ext, draft=False))
    if not after_close:
        draft = os.path.join(out_dir, dated_name(day, ext, draft=True))
        before = open(final, "rb").read() if os.path.exists(final) else None
        with open(draft, "w", encoding="utf-8") as fh:
            fh.write(text)
        after = open(final, "rb").read() if os.path.exists(final) else None
        if before != after:
            raise RuntimeError(f"pre-close run changed the final file {final}")
        print(
            f"[summary] wrote {draft} (before {day} 16:00 ET; "
            "final dated file not created or modified)",
            flush=True,
        )
        return draft
    if os.path.exists(final):
        print(FinalSignalExists(final), flush=True)
        raise FinalSignalExists(final)
    _exclusive_write(final, text)
    print(f"[summary] wrote {final}", flush=True)
    return final


def pct(v):
    try:
        return float(str(v).replace("%", "").replace("+", ""))
    except (TypeError, ValueError):
        return None


def render(rows, today):
    """Markdown body. `today` is the ET session date, not a signal change."""

    new = [r for r in rows if r["run_date"] == today]
    tracked = [r for r in rows if pct(r.get("ret_vs_open")) is not None
               and r["run_date"] != today]

    L = [f"# Excel-bot daily — {today}", ""]
    L.append(f"**{len(new)} new suggestions** today "
             f"(all-time: {len(rows)}; tracked with live returns: {len(tracked)})")
    L.append("")

    # ---- today's new signals
    L += ["## New suggestions", ""]
    if not new:
        L.append("_None — no cluster confirmed on the latest trading day._")
    else:
        L.append("| ticker | side | strategy | exit | ref close | signal colors |")
        L.append("|---|---|---|---|---|---|")
        for r in sorted(new, key=lambda r: (r["strategy"], r["ticker"])):
            L.append(f"| {r['ticker']} | {r['side']} | {r['strategy']} "
                     f"| {r['exit_rule']} | {r['ref_close']} | {r['signal_colors']} |")
    L.append("")

    # ---- strategy leaderboard (all tracked)
    L += ["## Live strategy scoreboard (all tracked suggestions, ret vs entry open)", ""]
    L.append("| strategy | n | mean | median | win% |")
    L.append("|---|---|---|---|---|")
    by_strat = {}
    for r in tracked:
        by_strat.setdefault(r["strategy"], []).append(pct(r["ret_vs_open"]))
    for s, vals in sorted(by_strat.items()):
        vals = sorted(vals)
        n = len(vals)
        mean = sum(vals) / n
        med = vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2
        win = sum(1 for v in vals if v > 0) / n * 100
        L.append(f"| {s} | {n} | {mean:+.2f}% | {med:+.2f}% | {win:.1f}% |")
    L.append("")

    # ---- movers among tracked
    def key(r):
        return pct(r["ret_vs_open"])
    top = sorted(tracked, key=key, reverse=True)[:10]
    bot = sorted(tracked, key=key)[:10]
    L += ["## Best open suggestions (ret vs entry open)", "",
          "| signal date | ticker | strategy | entry open | current | ret | days held |",
          "|---|---|---|---|---|---|---|"]
    for r in top:
        L.append(f"| {r['signal_date']} | {r['ticker']} | {r['strategy']} "
                 f"| {r['first_open']} | {r['current_price']} "
                 f"| {r['ret_vs_open']} | {r['days_held']} |")
    L += ["", "## Worst open suggestions", "",
          "| signal date | ticker | strategy | entry open | current | ret | days held |",
          "|---|---|---|---|---|---|---|"]
    for r in bot:
        L.append(f"| {r['signal_date']} | {r['ticker']} | {r['strategy']} "
                 f"| {r['first_open']} | {r['current_price']} "
                 f"| {r['ret_vs_open']} | {r['days_held']} |")
    L.append("")

    return "\n".join(L) + "\n"


def run(now=None, out_dir=OUT_DIR, sugg_path=SUGG):
    clock = session_clock(now)
    # Same ET clock as the 16:00 cutoff. On GitHub this matches the UTC
    # date daily_run stamps, for both the midday and after-close windows.
    today = clock.date().isoformat()
    with open(sugg_path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    text = render(rows, today)
    return write_dated(
        out_dir, today, ".md", text, after_close=is_after_close(clock),
    )


def main():
    try:
        run()
    except FinalSignalExists:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
