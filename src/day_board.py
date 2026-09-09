"""Trading-day process board — Stock Book readiness without running the Action.

Reads files already on disk (same contract as src.stock_book_diag), plus
the incremental land log from src.land_file. The .io page is static HTML
that polls raw.githubusercontent.com so a Pages rebuild is not required
to see file A land while file B is still running.

CLI: python -m src.day_board [--date YYYY-MM-DD] [--write]
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, stock_book_diag as diag
from . import stock_book_diag_signals as signals

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)
BOARD_DIR = ROOT / "data" / "day_board"
DASH_DIR = ROOT / "dashboard" / "day-board"
RAW_BASE = "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main"
PAGES_URL = "https://sroyaltyy.github.io/fullscan/dashboard/day-board/"
MAX_LANDS = 80


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _load_json(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _selections(date: str) -> dict:
    book = signals._load_book(date)  # noqa: SLF001 — same 1d lists as readiness
    buys, sells = signals._horizon_rows(book, "1d")  # noqa: SLF001
    flatten = ""
    fp = ROOT / "01_daily" / f"{date}_flatten_card.md"
    if fp.is_file():
        try:
            flatten = "\n".join(
                ln.strip() for ln in fp.read_text(encoding="utf-8").splitlines()
                if ln.strip()
            )[:700]
        except OSError:
            flatten = ""
    return {
        "buy_1d": [
            {"ticker": str(r.get("ticker") or ""),
             "score": r.get("score") or r.get("total")}
            for r in buys[:15]
        ],
        "sell_1d": [
            {"ticker": str(r.get("ticker") or ""),
             "score": r.get("score") or r.get("total")}
            for r in sells[:15]
        ],
        "flatten": flatten,
    }


def _disk_workflows(date: str) -> list[diag.WorkflowCheck]:
    """Same file contract as Stock Book readiness, but no GH / Pages fetch."""
    workflows: list[diag.WorkflowCheck] = []
    for spec in diag.workflow_specs(date, as_of=True):
        files = []
        for fspec in spec["files"]:
            if fspec.get("kind") == "pages_live":
                files.append(diag.FileCheck(
                    key=fspec["key"], name=fspec["name"],
                    path=fspec["rel"], role="optional",
                    status="SKIP",
                    reason="day-board reads main disk, not live Pages",
                    size=0, source=fspec.get("source") or "",
                ))
                continue
            files.append(diag._check_file(fspec, date))
        status, ready, n_ok, n_req, n_opt_ok, n_opt = diag.aggregate_status(files)
        workflows.append(diag.WorkflowCheck(
            key=spec["key"], name=spec["name"], yaml=spec["yaml"],
            status=status, inputs_ready=ready,
            n_req_ok=n_ok, n_req=n_req, n_opt_ok=n_opt_ok, n_opt=n_opt,
            files=files, gh_run=None,
        ))
    return workflows


def build(date: str, lands: list[dict] | None = None) -> dict:
    """Audit disk + merge prior land log. No GitHub API (no Action)."""
    workflows = _disk_workflows(date)
    book = next((w for w in workflows if w.key == "stock_book"), None)
    book_files = list(book.files) if book is not None else []
    book_json = next((f for f in book_files if f.key == "book_json"), None)
    book_written = bool(book_json and book_json.status == "OK")
    era_inputs_ok = all(f.status == "OK" for f in book_files if f.role == "input")
    from . import book_era
    historical = date < book_era.today_et()
    ranker_ready = era_inputs_ok or (historical and book_written)
    blockers = []
    for f in book_files:
        if f.role != "input" or f.status == "OK":
            continue
        blockers.append(f"{f.status} {f.name} `{f.path}` — {f.reason or f.status}")
    flags = [w.status for w in workflows]
    if all(s == "OK" for s in flags):
        overall = "OK"
    elif any(s == "FAIL" for s in flags) and not any(s == "OK" for s in flags):
        overall = "FAIL"
    elif any(s != "OK" for s in flags):
        overall = "PARTIAL" if any(s in ("OK", "PARTIAL") for s in flags) else "FAIL"
    else:
        overall = "FAIL"
    prev = _load_json(BOARD_DIR / f"{date}.json")
    if lands is None:
        lands = list(prev.get("lands") or [])
    processes = []
    n_ok = n_fail = n_partial = 0
    for w in workflows:
        files = []
        for f in w.files:
            files.append({
                "key": f.key,
                "name": f.name,
                "path": f.path,
                "role": f.role,
                "status": f.status,
                "reason": f.reason,
                "size": f.size,
            })
        processes.append({
            "key": w.key,
            "name": w.name,
            "yaml": w.yaml,
            "status": w.status,
            "inputs_ready": w.inputs_ready,
            "n_req_ok": w.n_req_ok,
            "n_req": w.n_req,
            "n_opt_ok": w.n_opt_ok,
            "n_opt": w.n_opt,
            "files": files,
        })
        if w.status == "OK":
            n_ok += 1
        elif w.status == "FAIL":
            n_fail += 1
        elif w.status == "PARTIAL":
            n_partial += 1
    return {
        "date": date,
        "generated_at": datetime.now(ET).isoformat(),
        "overall": overall,
        "ranker_ready": ranker_ready,
        "blockers": blockers,
        "counts": {
            "ok": n_ok, "partial": n_partial, "fail": n_fail,
            "n": len(processes),
        },
        "processes": processes,
        "selections": _selections(date),
        "lands": lands[-MAX_LANDS:],
        "href": {
            "pages": PAGES_URL,
            "readiness_action": (
                "https://github.com/SRoyaltyy/fullscan/actions/workflows/"
                "stock_book_diag.yml"
            ),
            "factor_mine": (
                "https://sroyaltyy.github.io/fullscan/dashboard/factor-mine/"
            ),
            "paper": "https://sroyaltyy.github.io/fullscan/dashboard/",
        },
    }


def _latest_dates(extra: str) -> list[str]:
    dates = set()
    if BOARD_DIR.is_dir():
        for p in BOARD_DIR.glob("20*.json"):
            if p.stem.count("-") == 2:
                dates.add(p.stem)
    dates.add(extra)
    return sorted(dates, reverse=True)[:40]


def merge_boards(theirs: dict, ours: dict) -> dict:
    """Union incremental lands when two jobs rewrote the same day JSON.

    2026-09-09: every land_* commit includes latest/today/dated board, so
    ubuntu book vs ECS weather rebase-conflicts on every step. Keep the
    newer disk snapshot and both sides' land log.
    """
    if not theirs:
        return dict(ours) if ours else {}
    if not ours:
        return dict(theirs)
    def ts(d: dict) -> str:
        return str(d.get("generated_at") or "")
    newer, older = (ours, theirs) if ts(ours) >= ts(theirs) else (theirs, ours)
    out = dict(newer)
    seen: set[tuple[str, str]] = set()
    lands: list[dict] = []
    for row in list(older.get("lands") or []) + list(newer.get("lands") or []):
        if not isinstance(row, dict):
            continue
        k = (str(row.get("key") or ""), str(row.get("at") or ""))
        if k in seen:
            continue
        seen.add(k)
        lands.append(row)
    lands.sort(key=lambda r: str(r.get("at") or ""))
    out["lands"] = lands[-MAX_LANDS:]
    ns = newer.get("selections") if isinstance(newer.get("selections"), dict) else {}
    osel = older.get("selections") if isinstance(older.get("selections"), dict) else {}
    if not (ns.get("buy_1d") or ns.get("sell_1d")) and (
            osel.get("buy_1d") or osel.get("sell_1d")):
        out["selections"] = osel
    if newer.get("ranker_ready") or older.get("ranker_ready"):
        out["ranker_ready"] = True
    return out


def merge_ours_dir(ours_dir: str) -> None:
    """Union OUR day_board snapshots into the copies currently on disk."""
    src = Path(ours_dir)
    if not src.is_dir():
        print(f"[day-board] merge-ours missing {ours_dir}")
        return
    BOARD_DIR.mkdir(parents=True, exist_ok=True)
    dates: set[str] = set()
    for folder in (BOARD_DIR, src):
        for p in folder.glob("20*.json"):
            if p.stem.count("-") == 2:
                dates.add(p.stem)
    if not dates:
        print("[day-board] merge-ours: no dated boards")
        return
    newest_date = ""
    newest_ts = ""
    for date in sorted(dates):
        theirs = _load_json(BOARD_DIR / f"{date}.json")
        ours = _load_json(src / f"{date}.json")
        merged = merge_boards(theirs, ours)
        if not merged:
            continue
        merged.setdefault("date", date)
        (BOARD_DIR / f"{date}.json").write_text(
            json.dumps(merged, indent=2), encoding="utf-8")
        gat = str(merged.get("generated_at") or "")
        if gat >= newest_ts:
            newest_ts = gat
            newest_date = date
    if not newest_date:
        newest_date = sorted(dates)[-1]
    board = _load_json(BOARD_DIR / f"{newest_date}.json")
    if board:
        write_json(board)
        n = len(board.get("lands") or [])
        print(f"[day-board] merged {ours_dir} -> {newest_date} ({n} lands)")


def write_json(board: dict) -> list[Path]:
    BOARD_DIR.mkdir(parents=True, exist_ok=True)
    date = str(board.get("date") or _today())
    day_p = BOARD_DIR / f"{date}.json"
    latest_p = BOARD_DIR / "latest.json"
    today_p = BOARD_DIR / "today.json"
    day_p.write_text(json.dumps(board, indent=2), encoding="utf-8")
    latest = {
        "date": date,
        "generated_at": board.get("generated_at"),
        "overall": board.get("overall"),
        "ranker_ready": board.get("ranker_ready"),
        "counts": board.get("counts") or {},
        "dates": _latest_dates(date),
        "href": f"{RAW_BASE}/data/day_board/{date}.json",
        "pages": PAGES_URL,
    }
    latest_p.write_text(json.dumps(latest, indent=2), encoding="utf-8")
    sel = board.get("selections") or {}
    today_p.write_text(json.dumps({
        "date": date,
        "generated_at": board.get("generated_at"),
        "overall": board.get("overall"),
        "ranker_ready": board.get("ranker_ready"),
        "counts": board.get("counts") or {},
        "buy_1d": sel.get("buy_1d") or [],
        "sell_1d": sel.get("sell_1d") or [],
        "flatten": sel.get("flatten") or "",
        "lands": (board.get("lands") or [])[-8:],
        "day_board": PAGES_URL,
    }, indent=2), encoding="utf-8")
    return [day_p, latest_p, today_p]


def note_land(date: str, *, key: str, title: str, files: list[dict],
              pushed: bool, preview: str = "",
              land: dict | None = None) -> list[Path]:
    """Merge one incremental land into the day JSON and rewrite latest."""
    prev = _load_json(BOARD_DIR / f"{date}.json")
    lands = list(prev.get("lands") or [])
    entry = land or {
        "key": key, "title": title, "at": datetime.now(ET).isoformat(),
        "pushed": pushed, "preview": preview, "files": files,
    }
    if "at" not in entry:
        entry["at"] = datetime.now(ET).isoformat()
    # Replace last row for the same key if it is the same step retry.
    if lands and lands[-1].get("key") == key:
        lands[-1] = entry
    else:
        lands.append(entry)
    board = build(date, lands=lands)
    return write_json(board)


def write_html() -> Path:
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    path = DASH_DIR / "index.html"
    path.write_text(DASH_HTML, encoding="utf-8")
    return path


DASH_HTML = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Day board — which processes ran</title>
<meta http-equiv="refresh" content="90">
<style>
 :root{
  --bg:#0f1420; --card:#171e2e; --line:#262f45; --line2:#232c42;
  --fg:#dfe6f2; --mut:#8b96ab; --dim:#66708a; --pos:#4ade80; --neg:#f87171;
  --gold:#fbbf24; --day:#121826;
 }
 *{box-sizing:border-box}
 html,body{margin:0;background:var(--bg);color:var(--fg);
   font:14px/1.45 -apple-system,Segoe UI,Roboto,sans-serif}
 .wrap{max-width:1080px;margin:0 auto;padding:16px 14px 48px}
 h1{font-size:20px;margin:0 0 4px}
 .sub{color:var(--mut);font-size:12px;margin:0 0 12px}
 .sub a{color:#93c5fd}
 .bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:0 0 12px}
 select,button{background:var(--card);color:var(--fg);border:1px solid var(--line);
   border-radius:8px;padding:8px 10px;font:13px/1.3 ui-monospace,Menlo,monospace}
 .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(132px,1fr));gap:8px;margin:0 0 14px}
 .card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:10px 12px}
 .card b{display:block;font-size:18px;margin-top:2px;font-variant-numeric:tabular-nums}
 .ok{color:var(--pos)} .fail{color:var(--neg)} .part{color:var(--gold)} .mut{color:var(--mut)}
 .proc{background:var(--card);border:1px solid var(--line);border-radius:10px;margin:0 0 10px;overflow:hidden}
 .proc h3{margin:0;padding:10px 12px;background:var(--day);font-size:14px;
   display:flex;flex-wrap:wrap;gap:8px 14px;align-items:baseline;justify-content:space-between}
 .proc table{width:100%;border-collapse:collapse;font-size:12.5px}
 th,td{padding:5px 10px;border-top:1px solid var(--line2);text-align:left;vertical-align:top}
 th{color:var(--mut);font-weight:600}
 td.path{font-family:ui-monospace,Menlo,monospace;font-size:11.5px;word-break:break-all}
 .preview{white-space:pre-wrap;font:12px/1.4 ui-monospace,Menlo,monospace;
   color:#c5d0e6;background:var(--day);border-radius:8px;padding:8px 10px;margin:8px 12px 12px}
 .land{border-left:3px solid var(--gold);padding:8px 12px;margin:0 0 8px;background:var(--card);
   border-radius:0 8px 8px 0}
 .land .when{color:var(--dim);font:11px/1.3 ui-monospace,Menlo,monospace}
 .pills{display:flex;flex-wrap:wrap;gap:6px;margin:0 0 12px}
 .pill{border:1px solid var(--line);border-radius:999px;padding:3px 10px;
   font:12px/1.3 ui-monospace,Menlo,monospace}
 .pill.buy{border-color:#166534;color:var(--pos)}
 .pill.sell{border-color:#7f1d1d;color:var(--neg)}
 .note{color:var(--dim);font-size:11.5px;margin-top:14px}
</style></head><body><div class="wrap">
<h1>Day board</h1>
<div class="sub">Same process list as Stock Book readiness, on
 <a href="https://sroyaltyy.github.io/fullscan/dashboard/">.io</a> —
 no Action click. Polls <code>main</code> so a file that just passed QC
 shows up here even if Pages has not rebuilt.
 · <a href="../factor-mine/">factor mine</a>
 · <a href="../">paper book</a>
 · <a href="../sleeve-merge/">sleeve merge</a></div>
<div class="bar">
  <label>Trading day
    <select id="dateSel"></select>
  </label>
  <button type="button" id="reload">Reload now</button>
  <span class="mut" id="stamp">loading…</span>
</div>
<div class="cards" id="cards"></div>
<h2>Today's selections</h2>
<div class="pills" id="sels"></div>
<pre class="preview" id="flatten" hidden></pre>
<h2>What was just pushed</h2>
<div id="lands"></div>
<h2>Processes</h2>
<div id="procs"></div>
<p class="note">OK = every required output exists and passes QC.
PARTIAL = some required files are good. FAIL = missing / empty / timeout stub.
Lands are write → QC → push of that file only. A later commit race cannot
erase a land that already reached <code>main</code>.</p>
</div>
<script>
const RAW = "https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/data/day_board";
const mark = {OK:"ok", PARTIAL:"part", FAIL:"fail"};
const fileMark = {OK:"✅", FAIL:"❌", MISSING:"⬜", SKIP:"➖", PARTIAL:"⚠️"};
function $(id){return document.getElementById(id)}
async function fetchJson(url){
  const u = url + (url.includes("?") ? "&" : "?") + "t=" + Date.now();
  const r = await fetch(u, {cache:"no-store"});
  if(!r.ok) throw new Error(r.status + " " + url);
  return r.json();
}
function tickers(rows, cls){
  return (rows||[]).map(r => {
    const t = (r.ticker||"").toUpperCase();
    if(!t) return "";
    return `<span class="pill ${cls}">${t}</span>`;
  }).join("");
}
function render(board){
  $("stamp").textContent = (board.generated_at||"") + " · overall " + (board.overall||"?");
  const c = board.counts||{};
  $("cards").innerHTML = [
    ["Overall", board.overall||"—", mark[board.overall]||"mut"],
    ["Ranker", board.ranker_ready ? "READY" : "BLOCKED", board.ranker_ready?"ok":"fail"],
    ["OK", c.ok??"—", "ok"],
    ["Partial", c.partial??"—", "part"],
    ["Fail / missing", c.fail??"—", "fail"],
  ].map(([k,v,cls]) => `<div class="card">${k}<b class="${cls}">${v}</b></div>`).join("");
  const sel = board.selections||{};
  const buys = tickers(sel.buy_1d, "buy");
  const sells = tickers(sel.sell_1d, "sell");
  $("sels").innerHTML = (buys || sells)
    ? (buys + sells)
    : `<span class="mut">No 1d BUY/SELL on disk for ${board.date}.</span>`;
  if(sel.flatten){
    $("flatten").hidden = false;
    $("flatten").textContent = sel.flatten;
  } else {
    $("flatten").hidden = true;
  }
  const lands = (board.lands||[]).slice().reverse();
  $("lands").innerHTML = lands.length ? lands.map(L => {
    const files = (L.files||[]).map(f =>
      `${f.ok?"✅":"❌"} ${f.path}${f.reason?" — "+f.reason:""}`).join("\n");
    const body = L.preview || files || "(no preview)";
    return `<div class="land"><div class="when">${L.at||""} · ${L.key||""} · pushed=${L.pushed?"yes":"no"}</div>
      <div><b>${L.title||L.key||""}</b></div>
      <pre class="preview">${esc(body)}</pre></div>`;
  }).join("") : `<p class="mut">Nothing landed incrementally yet for this date.</p>`;
  $("procs").innerHTML = (board.processes||[]).map(p => {
    const rows = (p.files||[]).map(f => `<tr>
      <td class="path">${esc(f.path)}</td>
      <td>${f.role||""}</td>
      <td>${fileMark[f.status]||f.status} ${f.status}</td>
      <td>${esc(f.reason||"")}${f.size? " · "+f.size+"B":""}</td>
    </tr>`).join("");
    return `<div class="proc"><h3><span>${esc(p.name)}</span>
      <span class="${mark[p.status]||"mut"}">${p.status} · req ${p.n_req_ok}/${p.n_req}</span></h3>
      <table><thead><tr><th>File</th><th>Need</th><th>Status</th><th>Detail</th></tr></thead>
      <tbody>${rows}</tbody></table></div>`;
  }).join("");
}
function esc(s){
  return String(s||"").replace(/[&<>]/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[ch]));
}
async function loadDate(date){
  const board = await fetchJson(`${RAW}/${date}.json`);
  render(board);
}
async function boot(){
  let latest;
  try { latest = await fetchJson(`${RAW}/latest.json`); }
  catch(e){ $("stamp").textContent = "Could not reach main JSON: "+e; return; }
  const dates = latest.dates || [latest.date];
  const sel = $("dateSel");
  sel.innerHTML = dates.map(d => `<option value="${d}">${d}</option>`).join("");
  const q = new URLSearchParams(location.search).get("date");
  const want = q && dates.includes(q) ? q : (latest.date || dates[0]);
  sel.value = want;
  await loadDate(want);
  sel.onchange = () => loadDate(sel.value);
  $("reload").onclick = () => loadDate(sel.value);
}
boot();
setInterval(() => { const d=$("dateSel").value; if(d) loadDate(d); }, 30000);
</script></body></html>
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--html", action="store_true",
                    help="Rewrite dashboard/day-board/index.html")
    ap.add_argument("--merge-ours", default="",
                    help="Directory of OUR day_board JSON to union into disk")
    args = ap.parse_args()
    if args.merge_ours:
        merge_ours_dir(args.merge_ours)
        return
    date = args.date or _today()
    board = build(date)
    print(
        f"[day-board] {date} overall={board['overall']} "
        f"ranker={'READY' if board['ranker_ready'] else 'BLOCKED'} "
        f"ok={board['counts']['ok']}/{board['counts']['n']}"
    )
    for p in board["processes"]:
        if p["status"] == "OK":
            continue
        print(f"  [{p['status']:<7}] {p['name']}")
    if args.write:
        paths = write_json(board)
        for p in paths:
            print(f"[day-board] wrote {p}")
    if args.html or args.write:
        hp = write_html()
        print(f"[day-board] wrote {hp}")


if __name__ == "__main__":
    main()
