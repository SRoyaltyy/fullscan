"""Past-day lock around Factor Mine recipe writes.

``src/factor_mine_book.py`` is byte-pinned, so the guard cannot live
inside ``write_action_mds``. This wrapper renders the same recipe text,
refuses a changed past day before any scoreboard file is replaced, then
calls the pinned writer and seals the new day.
"""
from __future__ import annotations

from pathlib import Path

from . import factor_mine_book as fmb
from . import past_day_lock as pdl


def render_recipe_texts(payload: dict, stats: list[dict], books: dict,
                        ) -> list[tuple[str, str]]:
    """Same recipe markdown ``write_action_mds`` is about to write."""
    recs = {r["name"]: r for r in (payload.get("recipes") or [])}
    by_stats = {s["name"]: s for s in stats}

    def rec_for(name: str, s: dict) -> dict:
        have = recs.get(name)
        if have:
            return have
        return {
            "name": name, "hold": s.get("hold"), "side": s.get("side"),
            "universe": s.get("universe"), "top_n": s.get("top_n"),
            "rank": s.get("rank"), "note": s.get("note"),
            "require": s.get("require") or {},
            "size": s.get("size") or "leftover",
            "sell": s.get("sell") or "list",
            "s_boost": s.get("s_boost") or "none",
            "members": s.get("members") or [],
            "weights": s.get("weights") or [],
            "net": s.get("net"),
            "pool": s.get("pool"),
            "explain": s.get("explain"),
        }

    rendered: list[tuple[str, str]] = []
    for name, book in books.items():
        row = by_stats.get(name)
        if not row:
            continue
        rendered.append((name, fmb.render_recipe_md(rec_for(name, row), row, book)))
    return rendered


def _live(dest_dir: Path) -> bool:
    return (
        pdl.in_repo(dest_dir)
        and dest_dir.resolve() == pdl.FACTOR_MINE_DIR.resolve()
    )


def locked_write_action_mds(original):
    """Guard, then the pinned writer, then seal. A refusal writes nothing."""

    def write_action_mds(payload, stats, books, featured, *,
                         out_dir=None, out_index=None, daily_md=None):
        dest_dir = Path(out_dir or fmb.OUT_DIR)
        rendered = render_recipe_texts(payload, stats, books)
        watermark = ""
        live = _live(dest_dir)
        if live:
            watermark = pdl.guard_factor_mine_dir(dest_dir, rendered)
        original(
            payload, stats, books, featured,
            out_dir=out_dir, out_index=out_index, daily_md=daily_md,
        )
        if live:
            pdl.seal_factor_mine_dir(dest_dir, rendered, watermark=watermark)

    write_action_mds._past_day_locked = True
    return write_action_mds


def install() -> None:
    current = fmb.write_action_mds
    if getattr(current, "_past_day_locked", False):
        return
    fmb.write_action_mds = locked_write_action_mds(current)


def main(argv: list[str] | None = None) -> int:
    install()
    from .factor_mine import main as mine_main
    return mine_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
