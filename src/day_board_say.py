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
    if not titles and not isinstance(data, dict):
        return None
    raw = data.get("raw_count") if isinstance(data, dict) else None
    usable = data.get("usable_count") if isinstance(data, dict) else None
    if raw is not None or usable is not None:
        said = f"{raw if raw is not None else len(items)} raw / {usable if usable is not None else len(titles)} usable"
    else:
        said = f"{len(items)} items"
    if titles:
        said += " · " + titles[0][:80]
    if not titles and raw is None:
        return None
    return {"said": said, "bullets": titles}


def news_actions(date: str) -> dict | None:
    data = _load_json(ROOT / "01_daily" / "news" / f"{date}_actions.json")
    if data is None:
        return None
    rows = []
    if isinstance(data, list):
        bag = data
    elif isinstance(data, dict):
        bag = []
        for k in ("actions", "items", "avoid", "buy", "sell",
                  "ticker_actions", "edge_actions"):
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
            why = it.get("why") or it.get("reason") or it.get("title") or it.get("event") or ""
            bit = " ".join(x for x in (str(act).upper(), str(t).upper(), str(why)) if x)[:120]
            if bit:
                rows.append(bit)
    if isinstance(data, dict) and (data.get("ticker_actions") or data.get("unique_events") is not None):
        n_t = len(data.get("ticker_actions") or [])
        n_e = data.get("unique_events")
        n_h = data.get("raw_headlines")
        tops = []
        for it in (data.get("ticker_actions") or [])[:8]:
            if isinstance(it, dict) and it.get("ticker"):
                side = str(it.get("side") or "").upper()
                tops.append(f"{it.get('ticker')}{(' '+side) if side else ''}")
        said = f"{n_t} ticker actions"
        if n_e is not None:
            said += f" · {n_e} events"
        if n_h is not None:
            said += f" · {n_h} headlines"
        if tops:
            said += " · " + ", ".join(tops)
        return {"said": said, "bullets": rows[:8]}
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
        digest = _finviz_digest(p)
        return digest
    if key == "catalyst":
        return news_actions(date)
    return None


SAID_LIMIT = 280
JSON_LOAD_MAX = 8_000_000


def _clip(text: str, n: int = SAID_LIMIT) -> str:
    text = re.sub(r"\s+", " ", (text or "").strip())
    if len(text) <= n:
        return text
    return text[: n - 1].rstrip() + "…"


def _load_json_file(path: Path):
    try:
        if not path.is_file() or path.stat().st_size > JSON_LOAD_MAX:
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def _titles(rows, keys: tuple[str, ...] = (
        "title", "headline", "event", "name", "one_line", "news_title")) -> list[str]:
    out: list[str] = []
    for row in rows or []:
        if isinstance(row, str) and row.strip():
            out.append(row.strip())
            continue
        if not isinstance(row, dict):
            continue
        for k in keys:
            v = row.get(k)
            if v:
                out.append(str(v).strip())
                break
    return [t for t in out if t]


def _tickers(rows, n: int = 8) -> list[str]:
    names: list[str] = []
    for row in rows or []:
        if isinstance(row, str) and row.strip():
            names.append(row.strip().upper())
        elif isinstance(row, dict):
            t = row.get("ticker") or row.get("symbol") or row.get("tkr")
            if t:
                names.append(str(t).strip().upper())
        if len(names) >= n:
            break
    return names


def _finviz_digest(path: Path) -> dict | None:
    data = _load_json_file(path)
    if not isinstance(data, dict):
        return None
    n = data.get("ticker_digest_count")
    sig = data.get("signal_count")
    tops = data.get("top_signal") or []
    names = _tickers(tops, 8)
    titles = _titles(tops, ("news_title", "digest", "title"))[:3]
    bits = [f"{n} tickers" if n is not None else "",
            f"{sig} signals" if sig is not None else ""]
    if names:
        bits.append("top " + ", ".join(names))
    said = " · ".join(b for b in bits if b)
    return {"said": said or "digest on disk", "bullets": titles}


def _market_digest(path: Path) -> dict | None:
    data = _load_json_file(path)
    if not isinstance(data, dict):
        return None
    if data.get("error"):
        return {"said": f"ERROR {data.get('error')}", "bullets": []}
    head = str(data.get("headline") or "").strip()
    names = [str(x).upper() for x in (data.get("named_leaders") or data.get("named_tickers") or [])[:6]]
    sent = str(data.get("finviz_sentiment") or "")
    bits = [b for b in (head, ("leaders " + ", ".join(names)) if names else "", sent) if b]
    return {"said": " · ".join(bits) or "market digest", "bullets": []}


def _events_file(path: Path) -> dict | None:
    data = _load_json_file(path)
    if not isinstance(data, dict):
        return None
    evs = data.get("events") or []
    titles = _titles(evs)[:4]
    risks = [str(x)[:90] for x in (data.get("top_risks") or [])[:2] if x]
    said = f"{len(evs)} events"
    if data.get("uncertainty"):
        said += f" · {data.get('uncertainty')} uncertainty"
    if data.get("summary"):
        said += " · " + str(data.get("summary"))[:140]
    return {"said": said, "bullets": titles or risks}


def _map_heat_json(path: Path) -> dict | None:
    data = _load_json_file(path)
    if not isinstance(data, dict):
        return None
    hot = [str(r.get("industry") or "") for r in (data.get("hot") or [])[:4] if isinstance(r, dict)]
    cold = [str(r.get("industry") or "") for r in (data.get("cold") or [])[:3] if isinstance(r, dict)]
    n = data.get("n_tickers")
    bits = [str(data.get("phase") or ""),
            f"{n} names" if n else "",
            ("hot " + ", ".join(x for x in hot if x)) if hot else "",
            ("cold " + ", ".join(x for x in cold if x)) if cold else ""]
    if data.get("macro_gate"):
        bits.append("macro_gate")
    return {"said": " · ".join(b for b in bits if b) or "heat tables", "bullets": []}


def _research_json(path: Path) -> dict | None:
    data = _load_json_file(path)
    if not isinstance(data, dict):
        return None
    cards = data.get("cards") or []
    actions: dict[str, int] = {}
    for c in cards:
        if isinstance(c, dict):
            actions[str(c.get("action") or "?")] = actions.get(str(c.get("action") or "?"), 0) + 1
    act = " ".join(f"{k}={v}" for k, v in list(actions.items())[:6])
    errs = data.get("morning_refresh_errors") or data.get("validation_errors") or []
    bits = [str(data.get("phase") or ""),
            f"{data.get('n_cards') or len(cards)} cards",
            f"refreshed {data.get('n_refreshed')}" if data.get("n_refreshed") is not None else "",
            "passthrough" if data.get("passthrough") else "",
            act]
    if errs:
        bits.append("err " + str(errs[0])[:80])
    para = str(data.get("one_paragraph") or "").strip()
    return {"said": " · ".join(b for b in bits if b), "bullets": [para] if para else []}


def _dossiers(path: Path) -> dict | None:
    data = _load_json_file(path)
    if not isinstance(data, dict):
        return None
    rows = data.get("dossiers") or []
    targets = [str(t.get("ticker") or "") for t in (data.get("targets") or []) if isinstance(t, dict)]
    errs = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        if r.get("error"):
            errs.append(f"{r.get('ticker')}: {r.get('error')}")
    said = (f"{data.get('n_ok') or 0}/{data.get('n_targets') or len(targets)} usable"
            f" · {data.get('routing') or 'dossiers'}")
    if targets:
        said += " · " + ", ".join(x for x in targets if x)
    return {"said": said, "bullets": errs[:4]}


def _preopen_qc_file(path: Path) -> dict | None:
    data = _load_json_file(path)
    if not isinstance(data, dict):
        return None
    items = data.get("items") or []
    bad = [i for i in items if isinstance(i, dict) and not i.get("ok")]
    bits = [f"all_ok={data.get('all_ok')}",
            f"sectors {data.get('sector_n_ok')}/{data.get('sector_n_total')}",
            f"{len(bad)} fail" if bad else "no fails"]
    names = [f"{i.get('kind')}:{i.get('reason')}" for i in bad[:6]]
    return {"said": " · ".join(str(b) for b in bits if b), "bullets": names}


def _preopen_status_file(path: Path) -> dict | None:
    data = _load_json_file(path)
    if not isinstance(data, dict):
        return None
    missing = data.get("missing_required") or []
    bits = [f"all_ok={data.get('all_ok')}",
            f"qc={data.get('qc_all_ok')}",
            f"grok={data.get('grok_ok')}",
            f"book={data.get('book_ok')}"]
    if missing:
        bits.append("missing " + ", ".join(str(x) for x in missing[:6]))
    return {"said": " · ".join(str(b) for b in bits), "bullets": []}


def _grok_review_file(path: Path) -> dict | None:
    data = _load_json_file(path)
    if not isinstance(data, dict):
        return None
    fails = data.get("fails") or []
    reasons = []
    for f in fails:
        if isinstance(f, dict):
            reasons.append(f"{f.get('reason') or ''} {f.get('path') or ''}".strip())
        else:
            reasons.append(str(f))
    notes = str(data.get("notes") or "").strip()
    said = f"ok={data.get('ok')}"
    if reasons:
        said += " · " + reasons[0]
    elif notes:
        said += " · " + notes[:160]
    return {"said": said, "bullets": [notes] if notes else reasons[:3]}


def _stock_book_file(path: Path) -> dict | None:
    data = _load_json_file(path)
    if not isinstance(data, dict):
        return None
    books = data.get("books") if isinstance(data.get("books"), dict) else {}
    one = books.get("1d") or {}
    buys = _tickers(one.get("buy") or [], 8)
    sells = _tickers(one.get("sell") or [], 6)
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    bias = meta.get("general_bias") or ""
    return {
        "said": f"1d BUY {', '.join(buys) or '—'} · SELL {', '.join(sells) or '—'}"
                + (f" · {bias}" if bias else ""),
        "bullets": [],
    }


def _green_file(path: Path) -> dict | None:
    data = _load_json_file(path)
    if data is None:
        return None
    rows = []
    if isinstance(data, dict):
        rows = data.get("rows") or data.get("tickers") or data.get("names") or []
    elif isinstance(data, list):
        rows = data
    names = _tickers(rows, 10)
    n = len(rows) if isinstance(rows, list) else "?"
    return {"said": f"{n} green · " + ", ".join(names), "bullets": []}


def _sector_predict_md(path: Path) -> dict | None:
    text = _read(path, limit=12_000)
    if not text:
        return None
    pipe = re.search(
        r"predicted_direction:\s+\**(\w+).*?predicted_magnitude_band:\s+\**(\w+).*?total_score:\s+\**(-?[0-9.]+)",
        text, re.S | re.I,
    )
    snap = re.search(
        r"Prediction:\s+([A-Z]+).*?total score\s+(-?[0-9.]+)", text, re.I,
    )
    if pipe:
        said = f"{pipe.group(1).upper()} {pipe.group(2)} {pipe.group(3)}"
    elif snap:
        said = f"{snap.group(1).upper()} {snap.group(2)}"
    else:
        sc = _scores_block(text)
        said = sc.get("HORIZON_3D") or ""
    first = next((ln.strip(" >#*") for ln in text.splitlines() if ln.strip() and not ln.startswith("#")), "")
    if not said and not first:
        return None
    return {"said": said or first[:160], "bullets": [first] if first and said else []}


def _judge_md(path: Path) -> dict | None:
    text = _read(path, limit=8_000)
    if not text:
        return None
    lines = [ln.strip(" -*") for ln in text.splitlines() if ln.strip() and not ln.startswith("#")]
    keep = [ln for ln in lines if len(ln) > 20][:4]
    if not keep:
        return None
    return {"said": keep[0][:160], "bullets": keep[1:4]}


def _plain_md(path: Path) -> dict | None:
    text = _read(path, limit=6_000)
    if not text:
        return None
    lines = [ln.strip(" >#*") for ln in text.splitlines() if ln.strip() and not ln.startswith("<!--")]
    keep = [ln for ln in lines if len(ln) > 12][:4]
    if not keep:
        return {"said": "markdown on disk", "bullets": []}
    return {"said": keep[0][:160], "bullets": keep[1:3]}


def summarize_file(rel: str, date: str, *, status: str = "",
                   reason: str = "") -> dict:
    """Short 'what is actually in this file' for one day-board row."""
    if status in ("MISSING", "SKIP"):
        return {"said": reason or status.lower(), "bullets": []}
    path = ROOT / rel
    name = path.name
    extract: dict | None = None
    if name.endswith("_finviz_digest.json"):
        extract = _finviz_digest(path)
    elif "finviz_market_digest" in name and name.endswith(".json"):
        extract = _market_digest(path)
    elif name.endswith("_parsed.json"):
        extract = news_parse(date)
    elif name.endswith("_actions.json"):
        extract = news_actions(date)
    elif name.endswith("_events.json"):
        extract = _events_file(path)
    elif name.endswith("_map_heat.json"):
        extract = _map_heat_json(path)
    elif name.endswith("_research_baseline.json"):
        extract = _research_json(path)
    elif name.endswith("_research.json"):
        extract = _research_json(path)
    elif name.endswith("_dossiers.json"):
        extract = _dossiers(path)
    elif name.endswith("_weather.json"):
        extract = weather(date)
    elif name.endswith("_preopen_qc.json"):
        extract = _preopen_qc_file(path)
    elif name.endswith("_preopen_status.json"):
        extract = _preopen_status_file(path)
    elif name.endswith("_grok_review.json"):
        extract = _grok_review_file(path)
    elif name.endswith("_stock_book.json"):
        extract = _stock_book_file(path)
    elif name.endswith("_green.json") or name.endswith("_suggestions.json"):
        extract = _green_file(path) or _stock_book_file(path)
    elif name.endswith("_board.json"):
        extract = sector_board(date)
    elif name.endswith("_predict.md") and "/general/" in rel.replace("\\", "/"):
        extract = general_predict(date)
    elif name.endswith("_predict.md"):
        extract = _sector_predict_md(path)
    elif name.endswith("_judge.md"):
        extract = _judge_md(path)
    elif name.endswith("_flatten_card.md"):
        extract = flatten_card(date)
    elif name.endswith(".csv"):
        extract = csv_head(path, n=8)
    elif name.endswith(".md"):
        extract = _plain_md(path)
    elif name.endswith(".json"):
        data = _load_json_file(path)
        if isinstance(data, dict):
            if data.get("error"):
                extract = {"said": f"ERROR {data.get('error')}", "bullets": []}
            else:
                keys = ", ".join(list(data.keys())[:10])
                extract = {"said": f"keys {keys}", "bullets": []}
        elif isinstance(data, list):
            extract = {"said": f"{len(data)} items", "bullets": []}
    if not extract:
        if reason:
            return {"said": reason, "bullets": []}
        if path.is_file():
            return {"said": f"{path.stat().st_size}B on disk", "bullets": []}
        return {"said": status.lower() or "missing", "bullets": []}
    said = _clip(str(extract.get("said") or ""))
    if reason and status == "FAIL" and reason not in said:
        said = _clip(f"{reason} · {said}" if said else reason)
    return {"said": said, "bullets": list(extract.get("bullets") or [])[:4]}
