#!/usr/bin/env python3
"""One-shot patcher: wire map_heat to finviz_calendars and stop ECS scrapes."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    mh = ROOT / "src" / "map_heat.py"
    src = mh.read_text(encoding="utf-8")
    src = src.replace(
        "from . import config, finviz_session, preopen",
        "from . import config, finviz_calendars, finviz_session, preopen",
        1,
    )
    start = src.find("FUTURES_KEEP = [")
    end = src.find("\n\n\ndef _session")
    if start >= 0 and end > start and "FUTURES_KEEP = finviz_calendars.FUTURES_KEEP" not in src:
        src = src[:start] + "FUTURES_KEEP = finviz_calendars.FUTURES_KEEP" + src[end:]
        print("FUTURES_KEEP: patched")
    else:
        print("FUTURES_KEEP: skip")

    start = src.find("def fetch_futures")
    end = src.find("def fetch_stock_news")
    if start >= 0 and end > start and "finviz_calendars.parse_futures_html" not in src[start:end]:
        src = src[:start] + (
            "def fetch_futures(sess: requests.Session) -> dict[str, dict]:\n"
            "    r = finviz_session.get(sess, [\"/futures.ashx\", \"/futures\"])\n"
            "    if r is None:\n"
            "        print(\"[map_heat] futures failed: Elite session empty/403/skipped\")\n"
            "        return {}\n"
            "    out = finviz_calendars.parse_futures_html(r.text)\n"
            "    print(f\"[map_heat] futures tiles: {len(out)}\")\n"
            "    return out\n\n\n"
            "def fetch_econ(sess: requests.Session, date: str) -> list[dict]:\n"
            "    return finviz_calendars.fetch_econ(sess, date)\n\n\n"
            "def fetch_earnings(sess: requests.Session, date: str) -> list[dict]:\n"
            "    return finviz_calendars.fetch_earnings(sess, date)\n\n\n"
            "def _parse_econ_html(html: str, asof: str) -> list[dict]:\n"
            "    return finviz_calendars.parse_econ_html(html, asof)\n\n\n"
            "def _parse_earnings_html(html: str, asof: str) -> list[dict]:\n"
            "    return finviz_calendars.parse_earnings_html(html, asof)\n\n\n"
            "def _numeric_surprise(actual: Any, forecast: Any) -> float | None:\n"
            "    return finviz_calendars._numeric_surprise(actual, forecast)\n\n\n"
        ) + src[end:]
        print("fetch wrappers: patched")
    else:
        print("fetch wrappers: skip")

    start = src.find("def _tape_from_futures")
    end = src.find("\n\n\ndef overlay_live")
    if start >= 0 and end > start and "finviz_calendars.tape_from_futures" not in src[start:end]:
        src = src[:start] + (
            "def _tape_from_futures(futures: dict) -> list[dict]:\n"
            "    return finviz_calendars.tape_from_futures(futures)\n\n\n"
            "def _calendar_fields(econ: list[dict], earns: list[dict], asof: str | None = None) -> dict:\n"
            "    return finviz_calendars.calendar_fields(econ, earns, asof)\n"
        ) + src[end:]
        print("tape/calendar: patched")
    else:
        print("tape/calendar: skip")

    src = src.replace(
        "    out.update(_calendar_fields(econ, earns))\n"
        "    out[\"tape\"] = _tape_from_futures(futures)\n"
        "    out[\"event_options\"] = fetch_event_options(out.get(\"earnings\") or [])\n"
        "    out[\"ticker_news\"] = ticker_news\n"
        "    out[\"major_news_tickers\"] = major_news_tickers\n",
        "    out.update(_calendar_fields(econ, earns, date))\n"
        "    tape = _tape_from_futures(futures)\n"
        "    if tape:\n"
        "        out[\"tape\"] = tape\n"
        "    elif out.get(\"tape\"):\n"
        "        print(\"[map_heat] live tape empty \u2014 keeping prior committed tape\")\n"
        "    else:\n"
        "        out[\"tape\"] = []\n"
        "    out[\"event_options\"] = fetch_event_options(out.get(\"earnings\") or [])\n"
        "    if ticker_news:\n"
        "        out[\"ticker_news\"] = ticker_news\n"
        "    if major_news_tickers:\n"
        "        out[\"major_news_tickers\"] = major_news_tickers\n",
    )
    src = src.replace(
        "    gates = _calendar_fields(econ, earns)\n",
        "    gates = _calendar_fields(econ, earns, date)\n",
    )
    mh.write_text(src, encoding="utf-8")

    yml = ROOT / ".github" / "workflows" / "map_heat_postclose.yml"
    text = yml.read_text(encoding="utf-8")
    if "ECS never scrapes Elite HTML" not in text:
        needle = "echo \"=== map_heat --force"
        i = text.find(needle)
        j = text.find("echo \"=== Grok captain research ===\"", i if i >= 0 else 0)
        if i < 0 or j < 0:
            raise SystemExit("postclose force block missing")
        text = (
            text[:i]
            + "echo \"=== clone GH-hosted map_heat (ECS never scrapes Elite HTML) ===\"\n"
            "          export FINVIZ_SKIP_LIVE=1\n"
            "          if [ -s \"01_daily/map_heat/${SOURCE}_map_heat.json\" ]; then\n"
            "            echo \"cloning $SOURCE map_heat \u2192 $TARGET\"\n"
            "            cp -f \"01_daily/map_heat/${SOURCE}_map_heat.json\" "
            "\"01_daily/map_heat/${TARGET}_map_heat.json\" || true\n"
            "            cp -f \"01_daily/map_heat/${SOURCE}_map_heat.md\" "
            "\"01_daily/map_heat/${TARGET}_map_heat.md\" || true\n"
            "          elif [ ! -s \"01_daily/map_heat/${TARGET}_map_heat.json\" ]; then\n"
            "            echo \"WARN: no $SOURCE heat to clone and $TARGET missing\"\n"
            "          fi\n"
            "          "
            + text[j:]
        )
        yml.write_text(text, encoding="utf-8")
        print("postclose yml: patched")
    else:
        print("postclose yml: skip")

    fa = ROOT / ".github" / "workflows" / "finviz_all.yml"
    ft = fa.read_text(encoding="utf-8")
    if 'cron: "10 1 * * 2-6"' not in ft:
        ft = ft.replace(
            "on:\n  workflow_dispatch:\n",
            "on:\n  schedule:\n"
            "    # 21:10 EDT post-close groups + tape on Azure, not Aliyun.\n"
            "    - cron: \"10 1 * * 2-6\"\n  workflow_dispatch:\n",
            1,
        )
        fa.write_text(ft, encoding="utf-8")
        print("finviz_all: patched")
    else:
        print("finviz_all: skip")

    po = ROOT / ".github" / "workflows" / "preopen_all.yml"
    pt = po.read_text(encoding="utf-8")
    if 'FINVIZ_SKIP_LIVE: "1"' not in pt:
        pt = pt.replace(
            '          GROK_ONLY: "1"\n        run: |\n          set -e\n',
            '          GROK_ONLY: "1"\n          FINVIZ_SKIP_LIVE: "1"\n        run: |\n          set -e\n',
            1,
        )
        po.write_text(pt, encoding="utf-8")
        print("preopen_all: patched")
    else:
        print("preopen_all: skip")

    sh = ROOT / "scripts" / "ecs_preopen.sh"
    st = sh.read_text(encoding="utf-8")
    if "FINVIZ_SKIP_LIVE=1" not in st:
        st = st.replace(
            "export PYTHONUNBUFFERED=1\n",
            "export PYTHONUNBUFFERED=1\nexport FINVIZ_SKIP_LIVE=1\n",
            1,
        )
        sh.write_text(st, encoding="utf-8")
        print("ecs_preopen: patched")
    else:
        print("ecs_preopen: skip")
    print("ALL PATCHES OK")


if __name__ == "__main__":
    main()
