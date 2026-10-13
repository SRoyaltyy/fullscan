import base64, gzip, json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import patch_fm_pending_day as p


def page(dates):
    b = base64.b64encode(gzip.compress(json.dumps({"dates": dates}).encode())).decode()
    return f'<html><body><div id="ovCalBox"></div><script>const B64 = "{b}";</script></body></html>'


def test_adds_pending_1013_with_ruling_label():
    out = p.patch(page(["2026-10-09", "2026-10-12"]), {"date": "2026-10-13", "n_ok": 5})
    assert "Newest day 2026-10-13" in out
    assert "pending ruling: GLM-written predict, not a decision yet" in out
    assert out.index("fmPendingDay") < out.index('id="ovCalBox"')
    assert p.patch(out, {"date": "2026-10-13"}) == out  # idempotent


def test_no_change_when_mined():
    pg = page(["2026-10-13"])
    assert p.patch(pg, {"date": "2026-10-13"}) == pg


def test_embedded_data_untouched():
    pg = page(["2026-10-12"])
    out = p.patch(pg, {"date": "2026-10-13"})
    assert p.embedded_last_date(out) == "2026-10-12"


def test_publish_script_reapplies_pending_patch():
    sh = (Path(__file__).resolve().parent / "publish_dashboard.sh").read_text(encoding="utf-8")
    assert "patch_fm_pending_day.py" in sh
