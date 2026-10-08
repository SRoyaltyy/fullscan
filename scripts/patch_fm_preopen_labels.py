#!/usr/bin/env python3
"""Add-only labels and the pre-open books on the Factor Mine page.

* New sleeves ``union_hot_n4_h1_preopen`` / ``union_hot_n4_holdup_preopen``:
  shown as $10k books from ``dashboard/factor-mine/preopen.json``, labelled
  'starts 10-08 open, no past days'. Days stay empty until the first sealed
  session is booked.
* Old ``union_hot_n4_h1`` / ``union_hot_n4_holdup``: 'picked after the close
  (evening list) — not knowable at 09:30'. ``union_hot_n4_h1`` also says it is
  the Factor Mine recipe, not the separate IRONCLAD h1 book.
* Panel 'Replay from pre-open inputs, not sealed' from
  ``dashboard/factor-mine/replay_preopen.json``.

Idempotent: a page that already has the marker is left as is. Nothing on the
page is removed or rewritten; the block is inserted at the top of the wrap.
"""
from __future__ import annotations

import html as H
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MARK = "<!-- fm-preopen-labels v1 -->"
NEW_LABEL = "starts 10-08 open, no past days"
PARENT_LABEL = ("picked after the close (evening list) — "
                "not knowable at 09:30")
H1_NOTE = ("Factor Mine recipe union_hot_n4_h1, not the separate "
           "IRONCLAD h1 book (research/hot_n4_clean_v4/forward_h1)")
SLEEVES = ("union_hot_n4_h1_preopen", "union_hot_n4_holdup_preopen")
PARENTS = {
    "union_hot_n4_h1_preopen": "union_hot_n4_h1",
    "union_hot_n4_holdup_preopen": "union_hot_n4_holdup",
}
LABELS = {
    "union_hot_n4_h1_preopen": NEW_LABEL,
    "union_hot_n4_holdup_preopen": NEW_LABEL,
    "union_hot_n4_h1": f"{PARENT_LABEL}. {H1_NOTE}",
    "union_hot_n4_holdup": PARENT_LABEL,
}
CAPITAL = 10000.0


def _e(x) -> str:
    return H.escape("" if x is None else str(x))


def _fills(rows) -> str:
    return ", ".join(f"{r.get('ticker')} {r.get('shares')}@{r.get('price')}"
                     for r in rows or []) or "—"


def replay_html(doc: dict | None) -> str:
    if not doc:
        return ("<p class='mut'>Replay file not found "
                "(data/factor_mine/replay_preopen/replay.json).</p>")
    out = []
    for name, rep in (doc.get("recipes") or {}).items():
        out.append(
            f"<h4>{_e(name)} <span class='mut'>from the {_e(rep.get('base_day'))} "
            f"end state ${_e(rep.get('base_equity'))}, held "
            f"{_e(', '.join(rep.get('base_holdings') or []) or 'none')}</span></h4>")
        out.append("<table><thead><tr><th>date</th><th>first-saved pre-open "
                   "list</th><th>ticket commit (ET)</th><th>S</th><th>buys</th>"
                   "<th>sells</th><th>fees</th><th>equity</th></tr></thead><tbody>")
        for d in rep.get("days") or []:
            picks = d.get("saved_picks") or []
            if picks:
                lst = _e(", ".join(picks))
                if d.get("red_morning"):
                    lst += " <b>(red morning S≤-3: no buys)</b>"
            else:
                lst = f"<b>no pre-open picks saved</b> — {_e(d.get('reason'))}"
            out.append(
                f"<tr><td>{_e(d.get('date'))}</td><td>{lst}</td>"
                f"<td>{_e(d.get('ticket_committed_at'))} "
                f"<code>{_e(str(d.get('ticket_commit') or '')[:9])}</code></td>"
                f"<td>{_e(d.get('s'))}</td><td>{_e(_fills(d.get('buys')))}</td>"
                f"<td>{_e(_fills(d.get('sells')))}</td><td>{_e(d.get('fees'))}</td>"
                f"<td>${_e(d.get('equity'))}</td></tr>")
        out.append("</tbody></table>")
    return "\n".join(out)


def _money(v) -> str:
    if v is None or v == "":
        return f"${CAPITAL:,.2f}"
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return f"${CAPITAL:,.2f}"


def books_html(pre: dict | None) -> str:
    sleeves = (pre or {}).get("sleeves") or {}
    rows = []
    for name in SLEEVES:
        s = sleeves.get(name) or {}
        days = s.get("days") or []
        last = days[-1] if days else {}
        why = "no sessions yet"
        if last:
            bits = [str(last.get("date") or "")]
            if last.get("sit"):
                bits.append("sit")
            picks = last.get("picks") or []
            if picks:
                bits.append("picks " + ", ".join(str(p) for p in picks))
            reasons = last.get("reasons") or []
            if reasons:
                bits.append("; ".join(str(r) for r in reasons))
            why = " · ".join(b for b in bits if b)
        rows.append(
            "<tr>"
            f"<td><code>{_e(name)}</code></td>"
            f"<td>{_e(s.get('label') or NEW_LABEL)}</td>"
            f"<td><code>{_e(s.get('parent') or PARENTS[name])}</code></td>"
            f"<td>{_e(_money(last.get('equity') if last else None))}</td>"
            f"<td>{len(days)}</td>"
            f"<td>{_e(why)}</td>"
            "</tr>"
        )
    empty = not any((sleeves.get(n) or {}).get("days") for n in SLEEVES)
    note = ("<p class='mut'>No past days. Each sleeve starts at $10,000 at the "
            "2026-10-08 open. The day table stays empty until that session is "
            "sealed before 09:30 ET and booked after the close.</p>"
            if empty else "")
    return (
        "<table><thead><tr><th>sleeve</th><th>label</th><th>parent</th>"
        "<th>equity</th><th>days</th><th>last session</th></tr></thead><tbody>"
        + "".join(rows) + "</tbody></table>" + note
    )


def _script(labels: str, embed: str) -> str:
    return """<script>
(function(){
  var L=__LABELS__;
  var EMBED=__EMBED__;
  function esc(x){
    return String(x==null?'':x).replace(/[&<>"]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c];});
  }
  function paint(doc){
    var host=document.getElementById('fm-preopen-books');
    if(!host || !doc) return;
    var sleeves=doc.sleeves||{};
    var names=['union_hot_n4_h1_preopen','union_hot_n4_holdup_preopen'];
    var html='<table><thead><tr><th>sleeve</th><th>label</th><th>parent</th><th>equity</th><th>days</th><th>last session</th></tr></thead><tbody>';
    var any=false;
    names.forEach(function(n){
      var s=sleeves[n]||{};
      var days=s.days||[];
      if(days.length) any=true;
      var last=days.length?days[days.length-1]:null;
      var eq=last&&last.equity!=null?Number(last.equity):10000;
      var why=last?(String(last.date||'')+(last.sit?' · sit':'')+((last.picks||[]).length?' · picks '+(last.picks||[]).join(', '):'')+((last.reasons||[]).length?' · '+(last.reasons||[]).join('; '):'')):'no sessions yet';
      html+='<tr><td><code>'+esc(n)+'</code></td><td>'+esc(s.label||L[n]||'')+'</td><td><code>'+esc(s.parent||'')+'</code></td><td>$'+eq.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})+'</td><td>'+days.length+'</td><td>'+esc(why)+'</td></tr>';
    });
    html+='</tbody></table>';
    if(!any) html+='<p class="mut">No past days. Each sleeve starts at $10,000 at the 2026-10-08 open. The day table stays empty until that session is sealed before 09:30 ET and booked after the close.</p>';
    host.innerHTML=html;
  }
  function nameOf(el){
    if(!el || (el.closest && el.closest('#fm-preopen-labels,#fm-replay-preopen'))) return '';
    if(el.dataset && el.dataset.fmlab) return '';
    var t='';
    if(el.tagName==='OPTION' || !el.children || el.children.length===0) t=(el.textContent||'').trim();
    else if(el.childNodes && el.childNodes[0] && el.childNodes[0].nodeType===3) t=el.childNodes[0].textContent.trim();
    return Object.prototype.hasOwnProperty.call(L,t) ? t : '';
  }
  function tag(el){
    var t=nameOf(el);
    if(!t) return;
    el.dataset.fmlab='1';
    if(el.tagName==='OPTION'){ el.textContent=t+' — '+L[t]; return; }
    var s=document.createElement('span');
    s.className='mut'; s.style.fontSize='0.85em'; s.textContent=' ('+L[t]+')';
    el.appendChild(s);
  }
  function tagAll(){
    document.querySelectorAll('code,td,th,option,b,a,span,h2,h3,h4,button,div').forEach(tag);
  }
  function addChips(){
    var bar=document.getElementById('sleeveBar');
    if(!bar || bar.querySelector('[data-fmpre]')) return;
    ['union_hot_n4_h1_preopen','union_hot_n4_holdup_preopen'].forEach(function(n){
      var b=document.createElement('button');
      b.type='button'; b.className='chip'; b.dataset.fmpre='1';
      b.textContent=n+' — '+(L[n]||'');
      b.onclick=function(ev){
        ev.stopPropagation();
        var box=document.getElementById('fm-preopen-labels');
        if(box) box.scrollIntoView({block:'nearest'});
      };
      bar.appendChild(b);
    });
  }
  function run(){ tagAll(); addChips(); }
  var busy=false;
  function later(){ if(busy) return; busy=true; setTimeout(function(){busy=false; run();},200); }
  function boot(){
    paint(EMBED);
    run();
    fetch('preopen.json',{cache:'no-store'}).then(function(r){return r.ok?r.json():null;}).then(function(doc){
      if(doc && doc.sleeves) paint(doc);
    }).catch(function(){});
  }
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',boot); else boot();
  new MutationObserver(later).observe(document.documentElement,{childList:true,subtree:true});
})();
</script>
""".replace("__LABELS__", labels).replace("__EMBED__", embed)


def block(replay: dict | None, pre: dict | None = None) -> str:
    labels = json.dumps(LABELS).replace("<", "\\u003c")
    embed = json.dumps(pre or {}, sort_keys=True).replace("<", "\\u003c")
    return f"""{MARK}
<section id="fm-preopen-labels" style="border:1px solid #fbbf24;padding:8px 12px;margin:8px 0">
<h3>Pre-open sleeves</h3>
<p class="mut">Same recipe, selection, and exits as the parent. Candidates come only from inputs committed before 09:30 ET: the pre-open strategy tickets (<code>look.source=look</code>), the morning weather S gate, and the prior day's state. A missing or late input sits, and the reason is logged. The evening candidate file is not an input.</p>
<div id="fm-preopen-books">
{books_html(pre)}
</div>
<ul>
<li><code>union_hot_n4_h1_preopen</code> — {NEW_LABEL}. Same recipe as union_hot_n4_h1, candidates only from inputs committed before 09:30 ET.</li>
<li><code>union_hot_n4_holdup_preopen</code> — {NEW_LABEL}. Same recipe as union_hot_n4_holdup, candidates only from inputs committed before 09:30 ET.</li>
<li><code>union_hot_n4_h1</code> — {PARENT_LABEL}. {H1_NOTE}.</li>
<li><code>union_hot_n4_holdup</code> — {PARENT_LABEL}.</li>
</ul>
</section>
<section id="fm-replay-preopen" style="border:2px dashed #b80;padding:8px 12px;margin:8px 0">
<h3>Replay from pre-open inputs, not sealed</h3>
<p class="mut">Research replay through the real engine (factor_mine_book.simulate_book), 09-29 to 10-06, one day at a time from the 09-28 end state on the scoreboard. Buys only from each day's first-saved pre-open ticket list (git show of its first commit, before 09:30 ET). No ticket is rebuilt. Red mornings (S≤-3) buy nothing. Fills at the 09:30 Yahoo open with Futubull fees. Not a scoreboard row and not in the past-day lock.</p>
{replay_html(replay)}
</section>
{_script(labels, embed)}
"""


def patch_html(page: str, doc: dict | None, pre: dict | None = None) -> str:
    if MARK in page:
        return page
    chunk = "\n" + block(doc, pre)
    needle = '<div class="wrap">'
    i = page.find(needle)
    if i >= 0:
        j = i + len(needle)
        return page[:j] + chunk + page[j:]
    lo = page.lower()
    i = lo.find("<body")
    if i < 0:
        return chunk + page
    j = page.find(">", i) + 1
    return page[:j] + chunk + page[j:]


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    target = Path(args[0] if args else ROOT / "dashboard/factor-mine/index.html")
    src = Path(args[1] if len(args) > 1 else ROOT / "dashboard/factor-mine/replay_preopen.json")
    pre_path = Path(args[2] if len(args) > 2 else ROOT / "dashboard/factor-mine/preopen.json")
    doc = json.loads(src.read_text(encoding="utf-8")) if src.is_file() else None
    pre = json.loads(pre_path.read_text(encoding="utf-8")) if pre_path.is_file() else None
    page = target.read_text(encoding="utf-8", errors="replace")
    new = patch_html(page, doc, pre)
    if new != page:
        target.write_text(new, encoding="utf-8")
        print("patched pre-open labels + books", target)
    else:
        print("pre-open labels already present", target)


if __name__ == "__main__":
    main()
