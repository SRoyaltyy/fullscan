"""Strategy comparison board — live hard-red + shipped catalog.

Run: python -m src.test_strategy_board
"""
from __future__ import annotations

from src.strategy_board import collect, render, write_md


def test_catalog_has_live_hard_red_and_shipped() -> None:
    rows = collect()
    ids = {r["id"] for r in rows}
    assert any(r.get("live") and r.get("family") == "sleeve merge"
               and ("robust" in (r.get("id") or "")
                    or "hard_red" in (r.get("id") or "")) for r in rows), ids
    assert "io_2w_size" in ids
    assert "excel_all" in ids or any(r["family"] == "excel" for r in rows)
    assert any(r.get("pr") == 67 for r in rows)
    assert any(r.get("pr") == 66 for r in rows)
    assert any(r.get("integrity") == "confirm" for r in rows)
    assert any(r.get("integrity") == "stitch" for r in rows)
    assert any(r.get("integrity") == "fill" for r in rows)
    assert len(rows) >= 20


def test_render_marks_integrity() -> None:
    html = render(collect())
    assert ("flatten_robust" in html or "flatten_hard_red" in html
            or "hard-red" in html.lower() or "robust" in html.lower())
    assert "fill" in html and "stitch" in html
    assert "Excel" in html or "excel" in html


def test_md_names_live_method() -> None:
    md = write_md(collect())
    assert "flatten_robust" in md or "flatten_hard_red" in md
    assert "fill" in md


def main() -> None:
    test_catalog_has_live_hard_red_and_shipped()
    test_render_marks_integrity()
    test_md_names_live_method()
    print("test_strategy_board: 3 ok")


if __name__ == "__main__":
    main()
