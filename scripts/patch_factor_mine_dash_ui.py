#!/usr/bin/env python3
"""Patch factor-mine dash template: taller/narrower chart, all-strats, hide-only toggles."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TPL = ROOT / "src" / "factor_mine_dash.html"


def sub(text: str, old: str, new: str, label: str) -> str:
    n = text.count(old)
    if n != 1:
        raise SystemExit(f"{label}: expected 1 match, got {n}")
    return text.replace(old, new)


def patch(text: str) -> str:
    if "function chartPool" in text and "height:420px" in text and "slice(0,8)" not in text:
        print("already patched")
        return text

    text = sub(
        text,
        "canvas{width:100%;max-width:100%;height:168px;background:var(--card);border:1px solid var(--line);border-radius:10px}\n .legend{display:flex;flex-wrap:wrap;gap:8px;margin:8px 0 0}\n .legend span{cursor:pointer;padding:2px 10px;border-radius:12px;border:1px solid #333;font-size:12px;user-select:none}",
        " .curve-box{max-width:820px;width:100%;margin:6px 0 12px}\n canvas{width:100%;max-width:100%;height:420px;background:var(--card);border:1px solid var(--line);border-radius:10px}\n .legend{display:flex;flex-wrap:wrap;gap:6px;margin:8px 0 0;max-width:820px}\n .legend span{cursor:pointer;padding:2px 10px;border-radius:12px;border:1px solid #333;font-size:12px;user-select:none}",
        "css",
    )
    text = sub(
        text,
        """@media (max-width:1100px){\n   canvas{height:150px}\n   .wrap{padding:8px 8px 24px}\n   h1{font-size:17px}\n }\n @media (max-width:720px){\n   canvas{height:140px}\n   .wrap{padding:8px 6px 20px}\n   h1{font-size:16px}\n }""",
        """@media (max-width:1100px){\n   canvas{height:380px}\n   .wrap{padding:8px 8px 24px}\n   h1{font-size:17px}\n }\n @media (max-width:720px){\n   canvas{height:320px}\n   .curve-box{max-width:100%}\n   .legend{max-width:100%}\n   .wrap{padding:8px 6px 20px}\n   h1{font-size:16px}\n }""",
        "media",
    )
    text = sub(
        text,
        '<canvas id="chart" height="280"></canvas>',
        '<canvas id="chart" height="420"></canvas>',
        "canvas tag",
    )
    text = sub(
        text,
        """function chipNames(){\n  const q=(document.getElementById('q')?.value||'').trim().toLowerCase();\n  const list=statsList();\n  if(q){\n    return list.filter(s=>s.name.toLowerCase().includes(q) || (s.note||'').toLowerCase().includes(q) || ((s.explain||{}).kid||'').toLowerCase().includes(q))\n      .map(s=>s.name).slice(0,36);\n  }\n  const out=[];\n  const add=n=>{ if(n && !out.includes(n)) out.push(n); };\n  if(bracketOn()){\n    list.slice().sort((a,b)=>Number(b.delta_ret_pct||-999)-Number(a.delta_ret_pct||-999))\n      .slice(0,12).forEach(s=>add(s.name));\n  } else {\n    ((D.combos&&D.combos.outperform)||[]).forEach(add);\n    (D.featured||[]).forEach(add);\n  }\n  list.slice().sort((a,b)=>Number(b.total_ret_pct)-Number(a.total_ret_pct))\n    .slice(0,12).forEach(s=>add(s.name));\n  list.slice(0,8).forEach(s=>add(s.name));\n  if(sleeveFilter!=='all') add(sleeveFilter);\n  return out.slice(0,36);\n}""",
        """function chipNames(){\n  const q=(document.getElementById('q')?.value||'').trim().toLowerCase();\n  const list=statsList();\n  if(q){\n    return list.filter(s=>s.name.toLowerCase().includes(q) || (s.note||'').toLowerCase().includes(q) || ((s.explain||{}).kid||'').toLowerCase().includes(q))\n      .map(s=>s.name);\n  }\n  const out=[];\n  const add=n=>{ if(n && !out.includes(n)) out.push(n); };\n  if(bracketOn()){\n    list.slice().sort((a,b)=>Number(b.delta_ret_pct||-999)-Number(a.delta_ret_pct||-999))\n      .forEach(s=>add(s.name));\n  } else {\n    ((D.combos&&D.combos.outperform)||[]).forEach(add);\n    (D.featured||[]).forEach(add);\n  }\n  list.slice().sort((a,b)=>Number(b.total_ret_pct)-Number(a.total_ret_pct))\n    .forEach(s=>add(s.name));\n  list.forEach(s=>add(s.name));\n  if(sleeveFilter!=='all') add(sleeveFilter);\n  return out;\n}""",
        "chipNames",
    )
    text = sub(
        text,
        Path("/home/workdir/artifacts/patch_factor_mine_dash_ui.py").read_text().split('text = sub(\n        text,\n        """function visibleKeys')[1] if False else OPEN_VISIBLE,
        NEW_VISIBLE,
        "chart js",
    )
    return text
