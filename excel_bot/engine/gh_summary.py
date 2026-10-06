"""Write a human-readable daily summary for the Excel-replica bot.

Run AFTER daily_run.py, from the excel_bot/ directory:
    python engine/gh_summary.py

Reads suggestions/suggestions.csv.

The morning schedule, the after-close schedule, and a manual dispatch
share this writer. The date on the file is the NYSE session in
America/New_York, never the UTC calendar date.

Before 16:00 ET on a session day it writes daily/{date}_excel_bot_draft.md
only and does not create the final. At or after 16:00 ET on a session
day it creates daily/{date}_excel_bot.md once. A weekend or full-day
holiday stamps the previous completed session and uses that same
write-once final. A second run refuses to overwrite it. Sibling
.csv/.json dated artifacts use the same rule. Zero network, zero
tokens — pure stdlib + the suggestions file.
"""
import csv
import os
from datetime import date, datetime, timedelta
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


def _nth_weekday(year, month, weekday, n):
    """n>0 is the nth weekday of the month (Mon=0). n=-1 is the last."""
    if n > 0:
        d = date(year, month, 1)
        d += timedelta(days=(weekday - d.weekday()) % 7)
        return d + timedelta(weeks=n - 1)
    if month == 12:
        d = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        d = date(year, month + 1, 1) - timedelta(days=1)
    d -= timedelta(days=(d.weekday() - weekday) % 7)
    return d


def _easter_gregorian(year):
    """Anonymous Gregorian Easter (Western). Same rule as src/skip_if_good."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    el = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * el) // 451
    month, day = divmod(h + el - 7 * m + 114, 31)
    return date(year, month, day + 1)


def _observed_nyse(d):
    """Saturday holiday closes Friday. Sunday holiday closes Monday."""
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


def is_nyse_holiday(d):
    """Full-day NYSE closures. Same set as src/skip_if_good.is_nyse_holiday."""
    y = d.year
    holidays = {
        _observed_nyse(date(y, 1, 1)),
        _nth_weekday(y, 1, 0, 3),
        _nth_weekday(y, 2, 0, 3),
        _easter_gregorian(y) - timedelta(days=2),
        _nth_weekday(y, 5, 0, -1),
        _observed_nyse(date(y, 6, 19)),
        _observed_nyse(date(y, 7, 4)),
        _nth_weekday(y, 9, 0, 1),
        _nth_weekday(y, 11, 3, 4),
        _observed_nyse(date(y, 12, 25)),
    }
    return d in holidays


def is_nyse_session(d):
    """Weekday that is not a full-day NYSE holiday."""
    return d.weekday() < 5 and not is_nyse_holiday(d)


class SessionStamp:
    """The NYSE date this run is allowed to name, and whether its close printed."""

    def __init__(self, session, write_final):
        self.session = session
        self.write_final = bool(write_final)

    def __repr__(self):
        kind = "final" if self.write_final else "draft"
        return f"SessionStamp({self.session.isoformat()}, {kind})"


def resolve_session(now=None):
    """Session stamp in America/New_York. The UTC date is not used.

    At or after 16:00 ET on an NYSE session, that day is the final.
    Before 16:00 ET on an NYSE session, that day has not closed: draft
    only, and no final is created for it. A weekend or full-day holiday
    stamps the previous completed session and writes that final once.
    """
    clock = session_clock(now)
    day = clock.date()
    if is_nyse_session(day):
        return SessionStamp(day, write_final=is_after_close(clock))
    previous = day - timedelta(days=1)
    while not is_nyse_session(previous):
        previous -= timedelta(days=1)
    return SessionStamp(previous, write_final=True)


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
    stamp = resolve_session(now)
    # ET session, including a GitHub start after midnight UTC that is
    # still the previous evening in New York, and a weekend/holiday
    # start that belongs to the previous completed session.
    today = stamp.session.isoformat()
    kind = "final" if stamp.write_final else "draft"
    print(f"[summary] session {today} America/New_York {kind}", flush=True)
    with open(sugg_path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    text = render(rows, today)
    return write_dated(
        out_dir, today, ".md", text, after_close=stamp.write_final,
    )


def main():
    try:
        run()
    except FinalSignalExists:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
