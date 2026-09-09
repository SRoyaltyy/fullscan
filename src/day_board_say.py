"""Short 'what the file said' extracts for the day board.

Keep these tiny. The page renders `said` + `bullets` only.
Bytes stay a stub detector: size < MIN_BYTES → no extract, status stays FAIL.
"""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MIN_BYTES = 80


def _read(path: Path, limit: int = 400_000) -> str:
    try:
        if not path.is_file() or path.stat().st_size < MIN_BYTES:
            return ""
        return path.read_text(encoding="utf-8", errors="replace")[:limit]
    except OSError:
        return ""


def _load_json(path: Path):
    raw = _read(path)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _scores_block(text: str) -> dict[str, str]:
    m = re.search(r"SCORES_BEGIN(.*?)SCORES_END", text, re.S)
    if not m:
        return {}
    out: dict[str, str] = {}
    for line in m.group(1).splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        out[k.strip()] = v.strip()
    return out


def general_predict(date: str) -> dict | None:
    p = ROOT / "01_daily" / "general" / f"{date}_predict.md"
    text = _read(p)
    if not text:
        return None
    sc = _scores_block(text)
    pipe = re.search(
        r"total_score:\s+\*?(-?[0-9.]+).*?predicted_direction:\s+\*?(\w+).*?"
        r"predicted_magnitude_band:\s+\*?(\w+).*?confidence_score:\s+([0-9.]+)",
        text,
        re.S,
    )
    direction = (sc.get("HORIZON_3D") or "").split(":")[0] or (
        pipe.group(2) if pipe else ""
    )
    if pipe:
        score, direction, mag, conf = (
            pipe.group(1), pipe.group(2), pipe.group(3), pipe.group(4),
        )
    else:
        score = mag = conf = ""
        snap = re.search(
            r"Prediction:\s+([A-Z]+).*?total score\s+(-?[0-9.]+)", text,
        )
        if snap:
            direction = snap.group(1).lower()
            score = snap.group(2)
    good = [x.strip() for x in re.split(r"[;•]\s*", sc.get("GOOD_NEWS", "")) if x.strip()][:4]
    bad = [x.strip() for x in re.split(r"[;•]\s*", sc.get("BAD_NEWS", "")) if x.strip()][:4]
    if not direction and not score:
        return None
    said = f"{(direction or '?').upper()} {mag or ''} {score}".strip()
    if conf:
        said += f" · conf {conf}"
    bullets = [f"bad: {b}" for b in bad[:3]] + [f"good: {g}" for g in good[:2]]
    return {"said": said, "bullets": bullets, "score": score, "direction": direction}


def sector_board(date: str) -> dict | None:
    data = _load_json(ROOT / "01_daily" / "sectors" / date / "_board.json")
    if not isinstance(data, dict):
        return None
    rows = []
    for s in data.get("sectors") or []:
        if not isinstance(s, dict):
            continue
        d = s.get("predicted_direction")
        if not d:
            rows.append(f"{s.get('etf') or s.get('sector')} missing")
            continue
        sc = s.get("total_score")
        try:
            sc_s = f"{float(sc):+.1f}"
        except (TypeError, ValueError):
            sc_s = ""
        rows.append(f"{s.get('etf') or s.get('sector')} {str(d).upper()} {sc_s}".strip())
    if not rows:
        return None
    n_up = sum(1 for s in data.get("sectors") or [] if s.get("predicted_direction") == "up")
    n_dn = sum(1 for s in data.get("sectors") or [] if s.get("predicted_direction") == "down")
    n_fl = sum(1 for s in data.get("sectors") or [] if s.get("predicted_direction") == "flat")
    return {
        "said": f"{n_up} up / {n_dn} down / {n_fl} flat",
        "bullets": rows,
    }


def weather(date: str) -> dict | None:
    data = _load_json(ROOT / "01_daily" / "weather" / f"{date}_weather.json")
    if not isinstance(data, dict):
        return None
    st = ((data.get("stances") or {}).get("sector") or {})
    counts: dict[str, int] = {}
    hostile = []
    for name, rec in st.items():
        if not isinstance(rec, dict):
            continue
        stance = str(rec.get("stance") or "unknown")
        counts[stance] = counts.get(stance, 0) + 1
        if stance == "hostile":
            hostile.append(name)
    if not counts:
        return None
    said = " · ".join(f"{k} {v}" for k, v in sorted(counts.items()) if v)
    bullets = [f"hostile: {', '.join(hostile)}"] if hostile else []
    sig = data.get("signals") or {}
    if sig.get("events_bear") or sig.get("events_bull"):
        bullets.append(f"events bull {sig.get('events_bull')} / bear {sig.get('events_bear')}")
    return {"said": said, "bullets": bullets}


def news_parse(date: str) -> dict | None:
    data = _load_json(ROOT / "01_daily" / "news" / f"{date}_parsed.json")
    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for k in ("usable_top", "all_items", "items", "events",
                  "headlines", "parsed"):
            if isinstance(data.get(k), list):
                items = data[k]
                break
    titles = []
    for it in items[:40]:
        if isinstance(it, str):
            titles.append(it)
            continue
        if not isinstance(it, dict):
            continue
        t = it.get("title") or it.get("headline") or it.get("event") or it.get("summary")
        if t:
            titles.append(str(t).strip())
    titles = [t for t in titles if t][:6]
    if not titles:
        return None
    return {"said": f"{len(items)} items", "bullets": titles}


def news_actions(date: str) -> dict | None:
    data = _load_json(ROOT / "01_daily" / "news" / f"{date}_actions.json")
    if data is None:
        return None
    rows = []
    if isinstance(data, list):
        bag = data
    elif isinstance(data, dict):
        bag = []
        for k in ("actions", "items", "avoid", "buy", "sell"):
            v = data.get(k)
            if isinstance(v, list):
                bag.extend(v)
        if not bag:
            bag = [data]
    else:
        bag = []
    for it in bag[:12]:
        if isinstance(it, str):
            rows.append(it)
        elif isinstance(it, dict):
            t = it.get("ticker") or it.get("symbol") or ""
            act = it.get("action") or it.get("ryg") or it.get("color") or it.get("side") or ""
            why = it.get("why") or it.get("reason") or it.get("title") or ""
            bit = " ".join(x for x in (str(act).upper(), str(t).upper(), str(why)) if x)[:120]
            if bit:
                rows.append(bit)
    if not rows:
        return None
    return {"said": f"{len(rows)} actions", "bullets": rows[:8]}


def flatten_card(date: str) -> dict | None:
    p = ROOT / "01_daily" / f"{date}_flatten_card.md"
    text = _read(p)
    if not text:
        return None
    cash = re.search(r"Cash leftover\s+\**\$?([0-9,]+\.?[0-9]*)", text, re.I)
    lots = re.search(r"Open lots\s+\**([0-9]+)", text, re.I)
    buys = re.search(r"priced mover BUYs\s+\**([0-9]+)", text, re.I)
    note = ""
    if "morning S missing" in text:
        note = "morning S missing"
    said_bits = []
    if buys:
        said_bits.append(f"{buys.group(1)} priced buys")
    if lots:
        said_bits.append(f"{lots.group(1)} lots")
    if cash:
        said_bits.append(f"cash ${cash.group(1)}")
    if note:
        said_bits.append(note)
    return {"said": " · ".join(said_bits) or "card on disk", "bullets": said_bits}


def csv_head(path: Path, n: int = 5) -> dict | None:
    raw = _read(path, limit=80_000)
    if not raw:
        return None
    try:
        rows = list(csv.reader(raw.splitlines()))
    except csv.Error:
        return None
    if len(rows) < 2:
        return None
    hdr = [h.strip() for h in rows[0]]
    tick_i = 0
    for i, h in enumerate(hdr):
        if h.lower() in ("ticker", "symbol", "tkr"):
            tick_i = i
            break
    names = []
    for r in rows[1:]:
        if tick_i < len(r) and r[tick_i].strip():
            names.append(r[tick_i].strip().upper())
        if len(names) >= n:
            break
    return {"said": f"{len(rows)-1} rows", "bullets": [", ".join(names)] if names else []}


def map_heat(date: str) -> dict | None:
    p = ROOT / "01_daily" / "map_heat" / f"{date}_map_heat.json"
    if p.is_file() and p.stat().st_size < MIN_BYTES:
        return {"said": f"STUB {p.stat().st_size}B", "bullets": ["json too small — treat as FAIL"]}
    data = _load_json(p)
    if data is None:
        md = _read(ROOT / "01_daily" / "map_heat" / f"{date}_research_baseline.md", limit=4000)
        if not md:
            return None
        return {"said": "baseline MD only", "bullets": [ln.strip()[:120] for ln in md.splitlines() if ln.strip()][:4]}
    if isinstance(data, dict) and len(data) <= 2:
        return {"said": "STUB object", "bullets": [str(list(data.keys())[:6])]}
    return {"said": "tables on disk", "bullets": []}


def summarize_process(key: str, date: str) -> dict | None:
    if key == "preopen":
        g = general_predict(date)
        s = sector_board(date)
        n = news_parse(date)
        bits = []
        bullets = []
        if g:
            bits.append("general " + g["said"])
            bullets.extend(g.get("bullets") or [])
        if s:
            bits.append("sectors " + s["said"])
            bullets.extend((s.get("bullets") or [])[:6])
        if n:
            bits.append("news " + n["said"])
            bullets.extend((n.get("bullets") or [])[:4])
        if not bits:
            return None
        return {"said": " · ".join(bits), "bullets": bullets[:10]}
    if key == "weather":
        return weather(date)
    if key == "ab":
        return csv_head(ROOT / "data" / "ab_checklist" / f"{date}_ab_checklist_enriched.csv")
    if key == "stock_book":
        return csv_head(ROOT / "data" / "join" / f"{date}_ranked.csv")
    if key == "postclose":
        return map_heat(date)
    if key == "finviz":
        p = ROOT / "01_daily" / "news" / f"{date}_finviz_digest.json"
        if p.is_file() and p.stat().st_size < MIN_BYTES:
            return {"said": f"STUB {p.stat().st_size}B", "bullets": ["digest json empty — use MD"]}
        return None
    if key == "catalyst":
        return news_actions(date)
    return None
