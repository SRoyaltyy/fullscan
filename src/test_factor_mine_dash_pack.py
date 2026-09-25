"""Sequential dashboard pack: designed_after stays out, untestable stays blank."""
from __future__ import annotations

import json
from pathlib import Path

from src.factor_mine_dash_pack import (
    pack_from_rows,
    render_changelog_html,
    render_sequential_html,
    write_pack,
)


def _row(name, date, futu, flat, fires, timing, created="2026-09-21"):
    return {
        "recipe": name,
        "recipe_created_date": created,
        "start_date": "2026-08-13",
        "D": date,
        "net_ret_futubull": futu,
        "net_ret_15bp": flat,
        "day_status": "pit_rebuilt" if timing == "true" else "incomplete_pit",
        "timing_clean": timing,
        "news_clean": "true",
        "reads_news": "false",
        "fires": fires,
        "untestable": "false",
    }


def test_designed_after_stays_out_of_the_real_total() -> None:
    rows = [
        _row("union_hot_n4_holdup", "2026-09-18", "10.0000", "11.0000", "1", "true"),
        _row("union_hot_n4_holdup", "2026-09-21", "10.0000", "12.0000", "2", "true"),
        _row("union_hot_n4_holdup", "2026-09-22", "0.0000", "0.0000", "1", "false"),
    ]
    pack = pack_from_rows(
        rows, created_on=lambda name, rec=None: "2026-09-21")
    rec = pack["recipes"][0]
    assert rec["n_designed_after"] == 1
    assert rec["n_real"] == 2
    assert rec["designed_after"]["futubull"] == 10.0
    assert rec["real"]["futubull"] == 10.0
    assert rec["timing_real"]["futubull"] == 10.0
    assert rec["timing_designed_after"]["futubull"] == 10.0
    assert rec["days"][0]["label"] == "designed_after"
    assert rec["days"][0]["timing_clean"] is True
    assert rec["days"][2]["timing_clean"] is False
    assert rec["days"][2]["label"] == "real"


def test_untestable_returns_are_blank() -> None:
    rows = [
        _row("quiet_h1", "2026-09-21", "0.0000", "0.0000", "0", "true",
             created="2026-08-13"),
    ]
    pack = pack_from_rows(
        rows, created_on=lambda name, rec=None: "2026-08-13",
        state_equity={"quiet_h1": {"date": "2026-09-21", "equity": 10000.0}})
    rec = pack["recipes"][0]
    assert rec["untestable"] is True
    assert rec["real"]["futubull"] is None
    assert rec["real"]["flat_15bp"] is None
    assert rec["book_pct"] is None
    assert rec["state_equity"] is None
    assert rec["days"][0]["futu"] is None
    html = render_sequential_html(pack)
    assert "quiet_h1" in html
    assert "0.000" not in html
    assert "10000" not in html


def test_baselines_and_changelog_page() -> None:
    rows = [_row("union_hot_n4_h1", "2026-08-13", "1.0000", "1.0000", "1", "true",
                 created="2026-08-13")]
    baselines = {
        "random4": {"rows": [{
            "name": "random4", "fee": "futubull", "universe": "with GLND",
            "start": "2026-08-13", "mean": -9.487, "p5": -22.907,
            "p50": -9.17, "p95": 2.757, "trades": 72,
        }]},
        "iwm": {"rows": [{
            "name": "iwm", "fee": "futubull", "universe": "buy-and-hold",
            "start": "2026-08-13", "mean": -7.183, "trades": 1,
        }]},
    }
    pack = pack_from_rows(
        rows, created_on=lambda name, rec=None: "2026-08-13",
        baselines=baselines)
    html = render_sequential_html(pack)
    assert "random4" in html and "iwm" in html
    assert "-9.487" in html and "-7.183" in html
    assert "timing_clean" in html and "designed_after" in html
    page = render_changelog_html("# Factor Mine change log\n\nNights that rewrote a past buy.\n")
    assert "Factor Mine change log" in page
    assert "Nights that rewrote a past buy." in page
    assert "<script" not in page.split("<pre>", 1)[1]


def test_write_pack_does_not_touch_state(tmp_path=None) -> None:
    import tempfile
    rows = [_row("union_h1", "2026-09-21", "1.5000", "1.5000", "1", "false",
                 created="2026-08-13")]
    pack = pack_from_rows(rows, created_on=lambda name, rec=None: "2026-08-13")
    with tempfile.TemporaryDirectory() as d:
        state = Path(d) / "state" / "union_h1"
        state.mkdir(parents=True)
        frozen = state / "2026-09-21.json"
        frozen.write_text('{"equity": 10150.0}\n', encoding="utf-8")
        before = frozen.read_bytes()
        dest = Path(d) / "dash"
        paths = write_pack(dest, pack=pack, changelog="Factor Mine change log\n")
        assert paths["sequential"].is_file()
        assert paths["changelog"].is_file()
        assert frozen.read_bytes() == before
        saved = json.loads(
            paths["sequential"].read_text(encoding="utf-8").split(
                'type="application/json">', 1)[1].split("</script>", 1)[0])
        assert saved["recipes"][0]["real"]["futubull"] == 1.5


def test_locked_csv_keeps_hot4_and_splits_holdup() -> None:
    from src.factor_mine_dash_pack import build_pack
    pack = build_pack()
    by = {row["name"]: row for row in pack["recipes"]}
    hot = by["union_hot_n4_h1"]
    assert hot["n_designed_after"] == 0
    assert hot["real"]["futubull"] == 24.991
    assert hot["timing_real"]["futubull"] == 28.623
    assert hot["state_equity"] == 12499.09
    hold = by["union_hot_n4_holdup"]
    assert hold["n_designed_after"] == 26
    assert hold["n_real"] == 4
    assert hold["real"]["futubull"] != hold["book_pct"]
    assert hold["state_equity"] == 15219.62
    assert hold["book_pct"] == 52.196
    dead = [row for row in pack["recipes"] if row["untestable"]]
    assert dead
    assert all(row["real"]["futubull"] is None for row in dead)
    assert all(row["book_pct"] is None for row in dead)
    bases = pack["baselines"]
    assert bases["random4"]["rows"]
    assert bases["iwm"]["rows"]


if __name__ == "__main__":
    test_designed_after_stays_out_of_the_real_total()
    test_untestable_returns_are_blank()
    test_baselines_and_changelog_page()
    test_write_pack_does_not_touch_state()
    test_locked_csv_keeps_hot4_and_splits_holdup()
    print("factor-mine dash pack tests passed")
