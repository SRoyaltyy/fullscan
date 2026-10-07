"""Past-day lock: a new day appends, a sealed day edit fails, a header may change."""
from __future__ import annotations

from pathlib import Path

from src.past_day_lock import (
    MANIFEST_PATH,
    PastDayLockError,
    assert_factor_mine,
    assert_manifest_prefix,
    assert_ticket,
    card_body,
    commit_open_tail_csv,
    describe_seeds,
    guard_csv,
    load_manifest,
    merge_open_tail_csv,
    prepare_flatten_card,
    seal_csv,
    seal_factor_mine,
    seal_flatten_card,
    seal_open_tail_csv,
    seal_ticket,
    sha256_text,
    skip_flatten_card_after_open,
)

ROOT = Path(__file__).resolve().parents[1]


def _expect_fail(fragment: str, fn) -> None:
    try:
        fn()
    except PastDayLockError as exc:
        text = str(exc)
        assert fragment in text, text
        return
    raise AssertionError(f"expected failure containing {fragment!r}")


def _scoreboard(day_rows: list[str], *, total: str = "-1.00", fills: int = 2,
                realized: str = "-10.00", audit: str = "PASS",
                fills_rows: list[str] | None = None) -> str:
    lines = [
        "# Factor mine action — `toy`",
        "",
        f"Cash book **{total}%** ($9,900) · signal-only was -1.00%. "
        f"Starts YES **0/1**. Fills {fills} · skips 0 · realized ${realized}.",
        "",
        "## State audit",
        "",
        f"**{audit}** · 0 violations. Close cash $9,900.00.",
        "",
        "## Each session (cash + holdings state)",
        "",
        "| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | "
        "Intraday $ | Bought | Sold | Close cash | Close equity | Close held |",
        "|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|",
        *day_rows,
        "",
        "## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)",
        "",
        "| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | "
        "Equity change (sells only) | Why | Cameras |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|---|",
        *(fills_rows or []),
    ]
    return "\n".join(lines) + "\n"


OLD_ROW = (
    "| 2026-10-05 | +0.10 | $10,000.00 | — | $10,000.00 | +0.00 | -10.00 | "
    "AAA | — | $9,990.00 | $9,990.00 | AAA×1 |"
)
NEW_ROW = (
    "| 2026-10-06 | +0.20 | $9,990.00 | AAA×1 | $9,990.00 | +0.00 | +5.00 | "
    "— | — | $9,995.00 | $9,995.00 | AAA×1 |"
)


def test_normal_append_passes(tmp: Path) -> None:
    manifest = tmp / "manifest.jsonl"
    buy = (
        "| 2026-10-06 09:30 ET | **BUY** | `AAA` | 1 | $10.00 | $1.00 | — | "
        "$9,989.00 | — | list | — |"
    )
    old = _scoreboard([OLD_ROW])
    new = _scoreboard(
        [OLD_ROW, NEW_ROW], total="-0.50", fills=3, realized="-5.00",
        fills_rows=[buy],
    )
    rows = load_manifest(manifest)
    assert_factor_mine("toy", old, new, watermark="2026-10-05", rows=rows)
    seal_factor_mine("toy", old, new, watermark="2026-10-05", manifest=manifest)
    sealed = load_manifest(manifest)
    assert sealed[0]["kind"] == "seed"
    assert sealed[0]["watermark"] == "2026-10-05"
    days = [row for row in sealed if row["kind"] == "day"]
    assert [row["date"] for row in days] == ["2026-10-06"]
    assert days[0]["name"] == "toy"
    from src.past_day_lock import locked_lines_by_date
    lines = locked_lines_by_date(new)["2026-10-06"]
    assert days[0]["sha256"] == sha256_text("\n".join(lines) + "\n")


def test_edit_past_day_fails(tmp: Path) -> None:
    manifest = tmp / "manifest.jsonl"
    old = _scoreboard([OLD_ROW])
    new = _scoreboard([OLD_ROW, NEW_ROW])
    seal_factor_mine("toy", old, new, watermark="2026-10-05", manifest=manifest)
    edited = new.replace(NEW_ROW, NEW_ROW.replace("AAA×1", "AAA×9"))
    rows = load_manifest(manifest)

    def check() -> None:
        assert_factor_mine("toy", new, edited, watermark="2026-10-05", rows=rows)

    _expect_fail("2026-10-06", check)
    assert load_manifest(manifest) == rows


def test_header_summary_update_passes(tmp: Path) -> None:
    manifest = tmp / "manifest.jsonl"
    old = _scoreboard([OLD_ROW])
    first = _scoreboard([OLD_ROW, NEW_ROW])
    seal_factor_mine("toy", old, first, watermark="2026-10-05", manifest=manifest)
    header = _scoreboard(
        [OLD_ROW, NEW_ROW], total="+4.20", fills=9, realized="+420.00", audit="PASS",
    )
    rows = load_manifest(manifest)
    assert_factor_mine("toy", first, header, watermark="2026-10-05", rows=rows)
    seal_factor_mine("toy", first, header, watermark="2026-10-05", manifest=manifest)
    assert load_manifest(manifest) == rows


def test_watermark_is_not_fingerprinted(tmp: Path) -> None:
    manifest = tmp / "manifest.jsonl"
    text = _scoreboard([OLD_ROW])
    seal_factor_mine("toy", "", text, watermark="2026-10-05", manifest=manifest)
    assert load_manifest(manifest) == []
    assert "14 Sep" not in describe_seeds() or "not fingerprinted" in describe_seeds()
    assert not MANIFEST_PATH.is_file() or "2026-09-14" not in MANIFEST_PATH.read_text(
        encoding="utf-8"
    )


def test_missed_day_is_not_filled(tmp: Path) -> None:
    manifest = tmp / "manifest.jsonl"
    old = _scoreboard([OLD_ROW])
    jumped = _scoreboard([OLD_ROW, NEW_ROW.replace("2026-10-06", "2026-10-08")])
    seal_factor_mine("toy", old, jumped, watermark="2026-10-05", manifest=manifest)
    gap = _scoreboard([
        OLD_ROW,
        NEW_ROW.replace("2026-10-06", "2026-10-07"),
        NEW_ROW.replace("2026-10-06", "2026-10-08"),
    ])
    rows = load_manifest(manifest)

    def check() -> None:
        assert_factor_mine("toy", jumped, gap, watermark="2026-10-05", rows=rows)

    _expect_fail("missed day", check)


def _write_flatten(day_dir: Path, manifest: Path, day: str, body: str) -> None:
    path = day_dir / f"{day}_flatten_card.md"
    kept = prepare_flatten_card(day, body, path, manifest=manifest)
    if kept is not None:
        path.write_text(kept, encoding="utf-8")
    seal_flatten_card(day, path, manifest=manifest)


def test_flatten_append_and_edit(tmp: Path) -> None:
    day_dir = tmp / "days"
    day_dir.mkdir()
    prior = day_dir / "2026-10-05_flatten_card.md"
    prior.write_text("# flatten_robust card — 2026-10-05\n\n| 16:00 ET | SELL | AAA |\n",
                     encoding="utf-8")
    manifest = tmp / "manifest.jsonl"
    body = (
        "# flatten_robust card — 2026-10-06\n\n"
        "_Generated 2026-10-06T09:25:00 — live `flatten_robust`._\n\n"
        "| 09:30 ET | BUY | BBB | 10 |\n"
    )
    _write_flatten(day_dir, manifest, "2026-10-06", body)
    sealed_dates = [row.get("date") for row in load_manifest(manifest) if row["kind"] == "day"]
    assert sealed_dates == []
    rerun = body.replace("BBB | 10", "BBB | 12")
    _write_flatten(day_dir, manifest, "2026-10-06", rerun)
    assert "BBB | 12" in (day_dir / "2026-10-06_flatten_card.md").read_text(encoding="utf-8")
    clock = rerun.replace("09:25:00", "09:40:00")
    path = day_dir / "2026-10-06_flatten_card.md"
    assert prepare_flatten_card("2026-10-06", clock, path, manifest=manifest) == clock
    next_day = rerun.replace("2026-10-06", "2026-10-07")
    _write_flatten(day_dir, manifest, "2026-10-07", next_day)
    sealed_dates = [row.get("date") for row in load_manifest(manifest) if row["kind"] == "day"]
    assert sealed_dates == ["2026-10-06"]
    edited = rerun.replace("BBB", "ZZZ")

    def check() -> None:
        prepare_flatten_card("2026-10-06", edited, path, manifest=manifest)

    _expect_fail("flatten_robust 2026-10-06", check)
    assert card_body(path.read_text(encoding="utf-8")).count("BBB") == 1


def test_paper_and_sleeve_fills_are_locked() -> None:
    paper = (ROOT / "src" / "paper_trade.py").read_text(encoding="utf-8")
    append = (ROOT / "src" / "paper_trade_append.py").read_text(encoding="utf-8")
    sleeve = (ROOT / "src" / "sleeve_merge.py").read_text(encoding="utf-8")
    book = (ROOT / "src" / "run_stock_book_all.py").read_text(encoding="utf-8")
    pre = (ROOT / "src" / "run_preopen_all.py").read_text(encoding="utf-8")
    live = (ROOT / "src" / "sleeve_merge_live.py").read_text(encoding="utf-8")
    boards = (ROOT / "src" / "publish_live_boards.py").read_text(encoding="utf-8")
    finish = (ROOT / ".github" / "workflows" / "preopen_finish_0908.yml").read_text(encoding="utf-8")
    # src/paper_trade.py is a pinned v4 engine file. The going-forward
    # CSV lock stays in the daily writer, outside those bytes.
    assert "past_day_lock" not in paper
    assert "commit_open_tail_csv" not in paper
    assert "commit_open_tail_csv" in append
    for record, filename in (
        ("paper_equity", "equity_curve.csv"),
        ("paper_trades", "trades.csv"),
        ("paper_roundtrips", "roundtrips.csv"),
        ("paper_skipped", "skipped.csv"),
    ):
        assert record in append
        assert filename in append
    assert "commit_open_tail_csv" in sleeve
    assert "prepare_flatten_card" in live
    assert "seal_flatten_card" in live
    assert "skip_flatten_card_after_open" in book
    note = describe_seeds()
    assert "data/paper/trades.csv" in note
    assert "data/sleeve_merge/trades.csv" in note
    assert "not locked" not in note
    assert "not fingerprinted" in note
    assert "src.paper_trade_append" in book
    assert "src.paper_trade_append" in pre
    assert "src.paper_trade_append" in boards
    assert "python -m src.paper_trade_append" in finish


def _lock_csv(record: str, old: str, new: str, column: str, manifest: Path) -> str:
    merged = merge_open_tail_csv(
        record, old, new, column=column, manifest=manifest,
    )
    seal_open_tail_csv(record, merged, column=column, manifest=manifest)
    return merged


def test_open_tail_csv_going_forward(tmp: Path) -> None:
    """Watermark days stay unhashed. The newest day rewrites. The prior day seals."""
    manifest = tmp / "manifest.jsonl"
    old = "date,ticker\r\n2026-10-01,AAA\r\n2026-10-02,BBB\r\n"
    rewritten = "date,ticker\r\n2026-10-01,ZZZ\r\n2026-10-02,QQQ\r\n"
    out = _lock_csv("paper_trades", old, rewritten, "date", manifest)
    assert "2026-10-01,AAA" in out
    assert "2026-10-02,QQQ" in out
    assert "ZZZ" not in out
    assert load_manifest(manifest) == []
    assert out.startswith("date,ticker\r\n2026-10-01,AAA\r\n")
    added = (
        "date,ticker\r\n2026-10-01,ZZZ\r\n2026-10-02,NOPE\r\n"
        "2026-10-03,CCC\r\n"
    )
    out = _lock_csv("paper_trades", out, added, "date", manifest)
    assert "2026-10-02,QQQ" in out
    assert "NOPE" not in out
    assert "2026-10-03,CCC" in out
    seed = [row for row in load_manifest(manifest) if row["kind"] == "seed"]
    assert seed[0]["watermark"] == "2026-10-02"
    assert [row.get("date") for row in load_manifest(manifest) if row["kind"] == "day"] == []
    nxt = (
        "date,ticker\r\n2026-10-01,Z\r\n2026-10-02,Z\r\n"
        "2026-10-03,CCC\r\n2026-10-04,DDD\r\n"
    )
    out = _lock_csv("paper_trades", out, nxt, "date", manifest)
    days = [row for row in load_manifest(manifest) if row["kind"] == "day"]
    assert [row["date"] for row in days] == ["2026-10-03"]
    assert days[0]["sha256"] == sha256_text("2026-10-03,CCC\n")
    assert "2026-10-01" not in [row["date"] for row in days]
    assert "2026-10-02" not in [row["date"] for row in days]
    same = nxt.replace("DDD", "EEE")
    out = _lock_csv("paper_trades", out, same, "date", manifest)
    assert "2026-10-04,EEE" in out
    assert [row["date"] for row in load_manifest(manifest) if row["kind"] == "day"] == ["2026-10-03"]
    bad = same.replace("2026-10-03,CCC", "2026-10-03,XXX")

    def check() -> None:
        merge_open_tail_csv("paper_trades", out, bad, column="date", manifest=manifest)

    _expect_fail("paper_trades 2026-10-03", check)
    path = tmp / "trades.csv"
    path.write_text(out, encoding="utf-8")
    commit_open_tail_csv(
        "paper_trades", path, same, column="date", manifest=manifest,
    )
    assert "2026-10-03,CCC" in path.read_text(encoding="utf-8")


def test_sleeve_and_roundtrip_open_tail(tmp: Path) -> None:
    manifest = tmp / "manifest.jsonl"
    old = (
        "cash_after,entry_date,exit_date,side,ticker\r\n"
        "1,2026-10-01,2026-10-02,BUY,AAA\r\n"
    )
    nxt = (
        "cash_after,entry_date,exit_date,side,ticker\r\n"
        "9,2026-10-01,2026-10-02,BUY,ZZZ\r\n"
        "2,2026-10-03,2026-10-03,BUY,BBB\r\n"
    )
    out = _lock_csv("sleeve_trades", old, nxt, "exit_date", manifest)
    assert "BUY,AAA" in out
    assert "ZZZ" not in out
    assert "BUY,BBB" in out
    assert [row.get("date") for row in load_manifest(manifest) if row["kind"] == "day"] == []
    later = (
        "cash_after,entry_date,exit_date,side,ticker\r\n"
        "9,2026-10-01,2026-10-02,BUY,ZZZ\r\n"
        "2,2026-10-03,2026-10-03,BUY,BBB\r\n"
        "3,2026-10-04,2026-10-04,BUY,CCC\r\n"
    )
    out = _lock_csv("sleeve_trades", out, later, "exit_date", manifest)
    days = [row["date"] for row in load_manifest(manifest) if row["kind"] == "day"]
    assert days == ["2026-10-03"]

    def smash() -> None:
        merge_open_tail_csv(
            "sleeve_trades", out, later.replace("BBB", "QQQ"),
            column="exit_date", manifest=manifest,
        )

    _expect_fail("sleeve_trades 2026-10-03", smash)

    trips_old = "status,sell_date,ticker\nclosed,2026-10-02,AAA\nopen,,BBB\n"
    trips_new = "status,sell_date,ticker\nclosed,2026-10-02,ZZZ\nclosed,2026-10-03,CCC\nopen,,DDD\n"
    trip_manifest = tmp / "trips.jsonl"
    trips = _lock_csv("paper_roundtrips", trips_old, trips_new, "sell_date", trip_manifest)
    assert "closed,2026-10-02,AAA" in trips
    assert "ZZZ" not in trips
    assert "open,,DDD" in trips
    trips_later = (
        "status,sell_date,ticker\nclosed,2026-10-02,ZZZ\n"
        "closed,2026-10-03,CCC\nclosed,2026-10-04,EEE\nopen,,FFF\n"
    )
    trips = _lock_csv("paper_roundtrips", trips, trips_later, "sell_date", trip_manifest)
    assert "open,,FFF" in trips
    assert [row["date"] for row in load_manifest(trip_manifest) if row["kind"] == "day"] == ["2026-10-03"]

    def smash_trip() -> None:
        merge_open_tail_csv(
            "paper_roundtrips", trips, trips_later.replace("CCC", "QQQ"),
            column="sell_date", manifest=trip_manifest,
        )

    _expect_fail("paper_roundtrips 2026-10-03", smash_trip)


def test_flatten_card_owner_after_open(tmp: Path) -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    et = ZoneInfo("America/New_York")
    card = tmp / "2026-10-06_flatten_card.md"
    early = datetime(2026, 10, 6, 9, 29, tzinfo=et)
    opened = datetime(2026, 10, 6, 9, 30, tzinfo=et)
    assert skip_flatten_card_after_open("2026-10-06", early, card) is False
    assert skip_flatten_card_after_open("2026-10-06", opened, card) is False
    card.write_text("# card\n", encoding="utf-8")
    assert skip_flatten_card_after_open("2026-10-06", early, card) is False
    assert skip_flatten_card_after_open("2026-10-06", opened, card) is True
    later = datetime(2026, 10, 6, 16, 5, tzinfo=et)
    assert skip_flatten_card_after_open("2026-10-06", later, card) is True


def test_csv_append_and_past_edit(tmp: Path) -> None:
    manifest = tmp / "manifest.jsonl"
    old = "date,sleeve,ticker\n2026-10-01,1d_top,AAA\n"
    new = old + "2026-10-02,1d_top,BBB\n"
    guard_csv("paper_trades", old, new, manifest=manifest)
    seal_csv("paper_trades", old, new, manifest=manifest)
    header = "date,sleeve,ticker\n2026-10-01,1d_top,AAA\n2026-10-02,1d_top,BBB\n"
    changed_old = "date,sleeve,ticker\n2026-10-01,1d_top,ZZZ\n2026-10-02,1d_top,BBB\n"
    guard_csv("paper_trades", header, changed_old, manifest=manifest)
    edited = header.replace("BBB", "QQQ")

    def check() -> None:
        guard_csv("paper_trades", header, edited, manifest=manifest)

    _expect_fail("paper_trades 2026-10-02", check)


def test_manifest_prefix() -> None:
    base = '{"kind":"seed","record":"toy","watermark":"2026-10-05"}\n'
    head = base + '{"date":"2026-10-06","kind":"day","name":"","record":"toy","sha256":"ab"}\n'
    assert_manifest_prefix(base, head)

    def changed() -> None:
        assert_manifest_prefix(base, base.replace("2026-10-05", "2026-10-04"))

    _expect_fail("manifest line changed", changed)


def test_workflow_runs_the_check() -> None:
    text = (ROOT / ".github" / "workflows" / "past_day_lock.yml").read_text(encoding="utf-8")
    assert "ubuntu-latest" in text
    assert "self-hosted" not in text
    assert "src.past_day_lock --check-against" in text
    assert "src.test_past_day_lock" in text
    assert "src.test_v4_engine_pins" in text
    assert "src.test_factor_mine_past_lock" in text
    assert "src/factor_mine.py" in text
    assert "00_grounding/futubull_fees.json" in text
    assert "research/hot_n4_clean_v4/protocol.py" in text
    mine = (ROOT / ".github" / "workflows" / "factor_mine.yml").read_text(encoding="utf-8")
    assert "src.factor_mine_past_lock" in mine
    yml = (ROOT / ".github" / "workflows" / "factor_mine.yml").read_text(encoding="utf-8")
    assert "data/past_day_lock/" in yml
    book = (ROOT / ".github" / "workflows" / "stock_book_all.yml").read_text(encoding="utf-8")
    assert 'runs-on: [self-hosted, ecs]' not in book.split("jobs:")[0]
    assert "data/past_day_lock/" in book


def _ticket_pass(day_dir: Path, manifest: Path, day: str, text: str, now) -> None:
    """Same lock branch as strategy_tickets.write, without rebuilding the book."""
    from src.strategy_tickets import dated_tickets_lock_reason

    path = day_dir / f"{day}_strategy_tickets.json"
    reason = dated_tickets_lock_reason(day, now) if path.is_file() else None
    keep = bool(reason and path.is_file() and path.read_text(encoding="utf-8") != text)
    assert_ticket(day, path, text, keep_existing=keep, manifest=manifest)
    if not (reason and path.is_file() and path.read_text(encoding="utf-8") != text):
        path.write_text(text, encoding="utf-8")
    seal_ticket(
        day, path,
        locked=dated_tickets_lock_reason(day, now) is not None,
        manifest=manifest,
    )


def test_morning_chain_replay(tmp: Path) -> None:
    """The live 2026-10-06 send journal must not lock this scratch session."""
    from src import strategy_tickets as st

    saved = st.paper_submit_journal
    st.paper_submit_journal = lambda date: tmp / "journals" / f"{date}_submit.json"
    try:
        _morning_chain_replay(tmp)
    finally:
        st.paper_submit_journal = saved


def _morning_chain_replay(tmp: Path) -> None:
    """Scratch copy of the 2026-10-05 books, then 10-06 twice and a rerun.

    10-05 stays the watermark. 10-06 stays open through same-day rewrites.
    10-07 seals 10-06. Editing that sealed card fails. Tickets may change
    at 08:07 and 09:07, and a post-09:30 rewrite keeps the sealed bytes.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    et = ZoneInfo("America/New_York")
    day_dir = tmp / "01_daily"
    ticket_dir = tmp / "tickets"
    day_dir.mkdir()
    ticket_dir.mkdir()
    manifest = tmp / "manifest.jsonl"
    card_src = (ROOT / "01_daily" / "2026-10-05_flatten_card.md").read_text(encoding="utf-8")
    ticket_src = (ROOT / "data" / "day_board" / "2026-10-05_strategy_tickets.json").read_text(
        encoding="utf-8"
    )
    (day_dir / "2026-10-05_flatten_card.md").write_text(card_src, encoding="utf-8")
    (ticket_dir / "2026-10-05_strategy_tickets.json").write_text(ticket_src, encoding="utf-8")

    def card(day: str, mark: str) -> str:
        text = card_src.replace("2026-10-05", day)
        return text.replace("16:00 **$", f"16:00 **{mark}$", 1)

    first = card("2026-10-06", "1")
    second = card("2026-10-06", "2")
    _write_flatten(day_dir, manifest, "2026-10-06", first)
    _write_flatten(day_dir, manifest, "2026-10-06", second)
    _write_flatten(day_dir, manifest, "2026-10-06", second)
    assert [row.get("date") for row in load_manifest(manifest) if row.get("kind") == "day"] == []
    third = card("2026-10-07", "3")
    _write_flatten(day_dir, manifest, "2026-10-07", third)
    _write_flatten(day_dir, manifest, "2026-10-07", card("2026-10-07", "4"))
    sealed = [row for row in load_manifest(manifest) if row.get("record") == "flatten_robust" and row.get("kind") == "day"]
    assert [row["date"] for row in sealed] == ["2026-10-06"]
    assert sealed[0]["sha256"] == sha256_text(card_body(second))
    on_disk = card_body((day_dir / "2026-10-06_flatten_card.md").read_text(encoding="utf-8"))
    assert sealed[0]["sha256"] == sha256_text(on_disk)

    def edit_past() -> None:
        prepare_flatten_card(
            "2026-10-06", second.replace("BUY", "SELL", 1),
            day_dir / "2026-10-06_flatten_card.md", manifest=manifest,
        )

    _expect_fail("flatten_robust 2026-10-06", edit_past)
    assert card_body((day_dir / "2026-10-06_flatten_card.md").read_text(encoding="utf-8")) == card_body(second)

    def at(day: str, hour: int, minute: int) -> datetime:
        y, m, d = (int(part) for part in day.split("-"))
        return datetime(y, m, d, hour, minute, tzinfo=et)

    morning = ticket_src.replace("2026-10-05", "2026-10-06", 1)
    body_a = morning
    body_b = morning + "\n"
    # The live 2026-10-06 submit journal freezes that date even at 08:07.
    # This replay is about the 09:30 clock, so it uses an empty journal.
    from unittest import mock
    from src import strategy_tickets

    def empty_journal(date: str) -> Path:
        return tmp / "paper_open" / f"{date}_submit.json"

    with mock.patch.object(strategy_tickets, "paper_submit_journal", empty_journal):
        _ticket_pass(ticket_dir, manifest, "2026-10-06", body_a, at("2026-10-06", 8, 7))
        _ticket_pass(ticket_dir, manifest, "2026-10-06", body_b, at("2026-10-06", 9, 7))
        held = (ticket_dir / "2026-10-06_strategy_tickets.json").read_text(encoding="utf-8")
        assert held == body_b
        after = body_b + "{\"rewritten\": true}\n"
        _ticket_pass(ticket_dir, manifest, "2026-10-06", after, at("2026-10-06", 9, 40))
        assert (ticket_dir / "2026-10-06_strategy_tickets.json").read_text(encoding="utf-8") == body_b
        ticket_days = [
            row["date"] for row in load_manifest(manifest)
            if row.get("record") == "strategy_tickets" and row.get("kind") == "day"
        ]
        assert ticket_days == ["2026-10-06"]
        next_morning = ticket_src.replace("2026-10-05", "2026-10-07", 1)
        _ticket_pass(ticket_dir, manifest, "2026-10-07", next_morning, at("2026-10-07", 8, 7))
        _ticket_pass(ticket_dir, manifest, "2026-10-07", next_morning + "\n", at("2026-10-07", 9, 7))
        held_next = (ticket_dir / "2026-10-07_strategy_tickets.json").read_text(encoding="utf-8")
        assert held_next == next_morning + "\n"
        assert "2026-10-07" not in [
            row["date"] for row in load_manifest(manifest)
            if row.get("record") == "strategy_tickets" and row.get("kind") == "day"
        ]

        def smash_ticket() -> None:
            assert_ticket(
                "2026-10-06", ticket_dir / "2026-10-06_strategy_tickets.json",
                body_b + "changed\n", keep_existing=False, manifest=manifest,
            )

        _expect_fail("strategy_tickets 2026-10-06", smash_ticket)


def test_sha_stable() -> None:
    assert sha256_text("abc") == sha256_text("abc")
    assert sha256_text("abc") != sha256_text("abd")


def main() -> None:
    import tempfile
    tests = [
        test_normal_append_passes,
        test_edit_past_day_fails,
        test_header_summary_update_passes,
        test_watermark_is_not_fingerprinted,
        test_missed_day_is_not_filled,
        test_flatten_append_and_edit,
        test_paper_and_sleeve_fills_are_locked,
        test_open_tail_csv_going_forward,
        test_sleeve_and_roundtrip_open_tail,
        test_flatten_card_owner_after_open,
        test_csv_append_and_past_edit,
        test_morning_chain_replay,
        test_manifest_prefix,
        test_workflow_runs_the_check,
        test_sha_stable,
    ]
    failed = 0
    for fn in tests:
        try:
            if fn.__code__.co_argcount:
                with tempfile.TemporaryDirectory() as raw:
                    fn(Path(raw))
            else:
                fn()
        except (Exception, SystemExit) as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
        else:
            print(f"ok {fn.__name__}")
    if failed:
        raise SystemExit(f"{failed} failed")
    print(f"{len(tests)} passed")


if __name__ == "__main__":
    main()
