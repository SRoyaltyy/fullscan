"""Part A (OHLC) + Part B1 checklist — loads known-good source + A15 tape recovery.

A15 GOOD when ALL of:
  * 2-day body R:G > 1.4
  * red wick avg > 1.15 × green wick avg over last 5 sessions
  * max green body > max red body over last 5 sessions

IMPORTANT: this file is a thin loader. It fetches the known-good implementation,
patches A15, then execs it. When run as `python -m src.ab_checklist`, it MUST
call main() explicitly — the exec'd source sees __name__ == 'src.ab_checklist',
so its own `if __name__ == "__main__"` block never fires.
"""
from __future__ import annotations

import re
import sys
import urllib.request
from pathlib import Path

_GOOD_SHA = "01d6380c8ed6dfff34afc78feed675386b26bf68"
_RAW = (
    f"https://raw.githubusercontent.com/SRoyaltyy/fullscan/{_GOOD_SHA}/src/ab_checklist.py"
)
_HERE = Path(__file__).resolve().parent
_CACHE = _HERE / "_ab_checklist_cached.py"

# Ensure repo root on path for package imports
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _fetch_good() -> str:
    if _CACHE.exists() and _CACHE.stat().st_size > 10_000:
        return _CACHE.read_text(encoding="utf-8")
    try:
        import subprocess

        src = subprocess.check_output(
            ["git", "show", f"{_GOOD_SHA}:src/ab_checklist.py"],
            cwd=_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        )
        if len(src) > 10_000:
            _CACHE.write_text(src, encoding="utf-8")
            return src
    except Exception:
        pass
    with urllib.request.urlopen(_RAW, timeout=60) as r:
        src = r.read().decode("utf-8")
    if len(src) < 10_000:
        raise RuntimeError("failed to load known-good ab_checklist source")
    _CACHE.write_text(src, encoding="utf-8")
    return src


def _apply_a15(t: str) -> str:
    if "A15_tape_recovery_setup" in t and "_five_day_tape" in t:
        return t

    old = '    "A13_red_body_vs_wick_2day",\n    "B01_eps_surprise",'
    new = (
        '    "A13_red_body_vs_wick_2day",\n'
        '    "A15_tape_recovery_setup",\n'
        '    "B01_eps_surprise",'
    )
    if "A15_tape_recovery_setup" not in t:
        if old not in t:
            raise RuntimeError("FEATURE_ORDER anchor missing in base ab_checklist")
        t = t.replace(old, new, 1)

    helper = '''
def _five_day_tape(df: pd.DataFrame) -> dict:
    """Last 5 sessions: max green body vs max red; avg wick red vs green; 2d body_rg."""
    win = df.tail(5)
    if len(win) < 2:
        return {"ok": False}
    sessions = []
    for idx, row in win.iterrows():
        o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
        body = c - o
        color = "GREEN" if body > 0 else ("RED" if body < 0 else "DOJI")
        wick = (h - max(o, c)) + (min(o, c) - l)
        sessions.append({
            "d": idx.date().isoformat() if hasattr(idx, "date") else str(idx)[:10],
            "body": body,
            "body_abs": abs(body),
            "color": color,
            "wick": wick,
        })
    greens = [s for s in sessions if s["color"] == "GREEN"]
    reds = [s for s in sessions if s["color"] == "RED"]
    max_g = max((s["body_abs"] for s in greens), default=0.0)
    max_r = max((s["body_abs"] for s in reds), default=0.0)
    avg_wick_g = float(np.mean([s["wick"] for s in greens])) if greens else np.nan
    avg_wick_r = float(np.mean([s["wick"] for s in reds])) if reds else np.nan
    last2 = sessions[-2:]
    bg = sum(s["body"] for s in last2 if s["color"] == "GREEN")
    br = sum(-s["body"] for s in last2 if s["color"] == "RED")
    body_rg_2 = (bg / br) if br > 1e-12 else (99.0 if bg > 0 else 1.0)
    return {
        "ok": True,
        "n": len(sessions),
        "start": sessions[0]["d"],
        "end": sessions[-1]["d"],
        "sessions": sessions,
        "max_green_body": max_g,
        "max_red_body": max_r,
        "max_green_gt_max_red": max_g > max_r,
        "avg_wick_green": avg_wick_g,
        "avg_wick_red": avg_wick_r,
        "red_wick_gt_green": (
            np.isfinite(avg_wick_r) and np.isfinite(avg_wick_g) and avg_wick_r > avg_wick_g * 1.15
        ),
        "body_rg_2day": body_rg_2,
        "trail": "; ".join(
            f"{s['d']}:{s['color']}:body={s['body']:+.4f}:wick={s['wick']:.4f}" for s in sessions
        ),
    }

'''
    if "_five_day_tape" not in t:
        t = t.replace(
            "def _part_a(ohlc: pd.DataFrame) -> dict:",
            helper + "def _part_a(ohlc: pd.DataFrame) -> dict:",
            1,
        )

    if "five = _five_day_tape" not in t:
        t = t.replace(
            "    pair = _pair_body_vol(df.iloc[-2], df.iloc[-1], d_prev, d_now)\n\n    # RVOL:",
            "    pair = _pair_body_vol(df.iloc[-2], df.iloc[-1], d_prev, d_now)\n"
            "    five = _five_day_tape(df)\n\n    # RVOL:",
            1,
        )
    if '"five_day": five' not in t:
        t = t.replace('"pair": pair,\n', '"pair": pair,\n        "five_day": five,\n', 1)

    if 'z["A15_tape_recovery_setup"]' not in t:
        a15 = '''
    five = a.get("five_day") or {}
    br = (a.get("pair") or {}).get("body_rg", np.nan)
    if (
        five.get("ok")
        and np.isfinite(br)
        and br > 1.4
        and five.get("red_wick_gt_green")
        and five.get("max_green_gt_max_red")
    ):
        z["A15_tape_recovery_setup"] = 1
    else:
        z["A15_tape_recovery_setup"] = 0

'''
        anchor = (
            '        z["A13_red_body_vs_wick_2day"] = -1 if rb >= rw else 1\n\n    return z\n'
        )
        if anchor not in t:
            raise RuntimeError("pass_a A13 anchor missing")
        t = t.replace(
            anchor,
            '        z["A13_red_body_vs_wick_2day"] = -1 if rb >= rw else 1\n'
            + a15
            + "    return z\n",
            1,
        )

    # value map line for A15 (best-effort)
    if '"A15_tape_recovery_setup":' not in t.split("def _value_map")[-1]:
        marker = '"A13_red_body_vs_wick_2day":'
        if marker in t:
            idx = t.find(marker)
            close = t.find("        }", idx)
            if close != -1:
                insert = (
                    '            "A15_tape_recovery_setup": (\n'
                    '                "n/a" if not (a.get("five_day") or {}).get("ok") else (\n'
                    "                    f\"body_rg_2d={(a.get('five_day') or {}).get('body_rg_2day', p.get('body_rg'))} need>1.4; \"\n"
                    "                    f\"red_wick_gt_green={(a.get('five_day') or {}).get('red_wick_gt_green')} \"\n"
                    "                    f\"5d trail={(a.get('five_day') or {}).get('trail')}\"\n"
                    "                )\n"
                    "            ),\n"
                )
                t = t[:close] + insert + t[close:]

    compile(t, "ab_checklist.py", "exec")
    return t


# Rewrite relative imports so this works under python -m src.*
_src = _apply_a15(_fetch_good())
_src = _src.replace("from . import config", "from src import config")
_src = _src.replace("from . import price_store as ps", "from src import price_store as ps")

_g = globals()
_g["__name__"] = "src.ab_checklist"
_g["__package__"] = "src"
_g["__file__"] = str(Path(__file__).resolve())
exec(compile(_src, __file__, "exec"), _g)

# The exec'd body defines main()/run() but its `if __name__ == "__main__"`
# never fires because we forced __name__ = "src.ab_checklist" above.
# Without this call, `python -m src.ab_checklist` is a no-op and leaves
# whatever stale CSV is already on disk (often a 1-row AAPL ab_one output).
if __name__ == "__main__":
    main()  # noqa: F821  — injected by exec above
