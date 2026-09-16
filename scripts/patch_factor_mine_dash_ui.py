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
        """function visibleKeys(){\n  if(sleeveFilter!=='all') return [sleeveFilter];\n  const prefer=[...(D.featured||[])];\n  statsList().slice().sort((a,b)=>Number(b.total_ret_pct)-Number(a.total_ret_pct))\n    .forEach(s=>{ if(!prefer.includes(s.name)) prefer.push(s.name); });\n  return prefer.filter(k=>!hidden.has(k) && (seriesOf(k)||[]).some(v=>v!=null)).slice(0,8);\n}\n\nfunction draw(){\n  const cv=document.getElementById('chart'),ctx=cv.getContext('2d');\n  const W=cv.width=cv.clientWidth*2,H=cv.height=Math.max(280, Math.round(cv.clientHeight*2));\n  ctx.clearRect(0,0,W,H);\n  const vis=visibleKeys();\n  let lo=Infinity,hi=-Infinity;\n  vis.forEach(k=>seriesOf(k).forEach(v=>{if(v!=null){lo=Math.min(lo,v);hi=Math.max(hi,v);}}));\n  if(lo===Infinity)return;\n  const pad=(hi-lo)*0.06||1; lo-=pad; hi+=pad;\n  const dates=D.dates||[];\n  const X=i=>40+i/(Math.max(1,dates.length-1))*(W-70);\n  const Y=v=>H-30-(v-lo)/(hi-lo)*(H-60);\n  ctx.strokeStyle='#2a3450';ctx.fillStyle='#66708a';ctx.font='20px sans-serif';\n  for(let g=0;g<=4;g++){\n    const v=lo+(hi-lo)*g/4;\n    ctx.beginPath();ctx.moveTo(40,Y(v));ctx.lineTo(W-30,Y(v));ctx.stroke();\n    ctx.fillText('$'+ (v/1000).toFixed(1)+'k',2,Y(v)+6);\n  }\n  dates.forEach((d,i)=>{if(i%Math.ceil(dates.length/6)===0)ctx.fillText(String(d).slice(5),X(i)-18,H-8);});\n  vis.forEach((k,ii)=>{\n    ctx.strokeStyle=COLORS[ii%COLORS.length];\n    ctx.lineWidth=sleeveFilter===k?3:1.8;\n    ctx.beginPath(); let started=false;\n    seriesOf(k).forEach((v,j)=>{if(v==null)return; started?ctx.lineTo(X(j),Y(v)):ctx.moveTo(X(j),Y(v)); started=true;});\n    ctx.stroke();\n  });\n}\n\nfunction renderLegend(){\n  const vis=visibleKeys();\n  const leg=document.getElementById('legend');\n  leg.innerHTML=\"\";\n  vis.forEach((k,ii)=>{\n    const s=document.createElement('span');\n    s.textContent=k;\n    s.style.borderColor=COLORS[ii%COLORS.length];\n    s.style.color=COLORS[ii%COLORS.length];\n    s.onclick=()=>{hidden.has(k)?hidden.delete(k):hidden.add(k); renderAll();};\n    s.style.opacity=hidden.has(k)?0.3:1;\n    leg.appendChild(s);\n  });\n}""",
        "PLACEHOLDER_CHART_JS_OLD",
        "chart js placeholder",
    )
    return text


def main() -> int:
    raw = TPL.read_text(encoding="utf-8")
    new = patch(raw)
    if new != raw:
        TPL.write_text(new, encoding="utf-8")
        print(f"patched {TPL} ({len(raw)} -> {len(new)})")
    else:
        print(f"unchanged {TPL}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
