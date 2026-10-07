#!/usr/bin/env python3
"""Add-only labels and the pre-open replay panel on the Factor Mine page.

* New sleeves ``union_hot_n4_h1_preopen`` / ``union_hot_n4_holdup_preopen``:
  'starts 10-08 open, no past days'.
* Old ``union_hot_n4_h1`` / ``union_hot_n4_holdup``: 'picked after the close
  (evening list), not knowable at 09:30'. ``union_hot_n4_h1`` also says it is
  the Factor Mine recipe, not the separate IRONCLAD h1 book.
* Panel 'Replay from pre-open inputs, not sealed' from
  ``dashboard/factor-mine/replay_preopen.json``.

Idempotent: a page that already has the marker is left as is. Nothing on the
page is removed or rewritten; the labels are appended next to the names.
"""
from __future__ import annotations

import html as H
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MARK = "<!-- fm-preopen-labels v1 -->"
NEW_LABEL = "starts 10-08 open, no past days"
PARENT_LABEL = "picked after the close (evening list), not knowable at 09:30"
H1_NOTE = "Factor Mine recipe, not the IRONCLAD h1 book"
LABELS = {
    "union_hot_n4_h1_preopen": NEW_LABEL,
    "union_hot_n4_holdup_preopen": NEW_LABEL,
    "union_hot_n4_h1": f"{PARENT_LABEL} · {H1_NOTE}",
    "union_hot_n4_holdup": PARENT_LABEL,
}


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


def block(doc: dict | None) -> str:
    labels = json.dumps(LABELS)
    return f"""{MARK}
<section id="fm-preopen-labels" style="border:1px solid #888;padding:8px 12px;margin:8px 0">
<h3>Pre-open sleeves</h3>
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
{replay_html(doc)}
</section>
<script>
(function(){{
  var L={labels};
  function tag(el){{
    if(el.dataset && el.dataset.fmlab) return;
    var t=(el.textContent||'').trim();
    if(!Object.prototype.hasOwnProperty.call(L,t)) return;
    if(el.closest && el.closest('#fm-preopen-labels,#fm-replay-preopen')) return;
    el.dataset.fmlab='1';
    if(el.tagName==='OPTION'){{ el.textContent=t+' — '+L[t]; return; }}
    var s=document.createElement('span');
    s.className='mut'; s.style.fontSize='0.85em'; s.textContent=' ('+L[t]+')';
    el.appendChild(s);
  }}
  function run(){{
    document.querySelectorAll('code,td,th,option,b,a,span,h2,h3,h4,button,div').forEach(function(el){{
      if(el.children.length===0 || el.tagName==='OPTION') tag(el);
    }});
  }}
  var busy=false;
  function later(){{ if(busy) return; busy=true; setTimeout(function(){{busy=false; run();}},200); }}
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',run); else run();
  new MutationObserver(later).observe(document.documentElement,{{childList:true,subtree:true}});
}})();
</script>
"""


def patch_html(page: str, doc: dict | None) -> str:
    if MARK in page:
        return page
    lo = page.lower()
    i = lo.find("<body")
    if i < 0:
        return block(doc) + page
    j = page.find(">", i) + 1
    return page[:j] + "\n" + block(doc) + page[j:]


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    target = Path(args[0] if args else ROOT / "dashboard/factor-mine/index.html")
    src = Path(args[1] if len(args) > 1 else ROOT / "dashboard/factor-mine/replay_preopen.json")
    doc = json.loads(src.read_text(encoding="utf-8")) if src.is_file() else None
    page = target.read_text(encoding="utf-8", errors="replace")
    new = patch_html(page, doc)
    if new != page:
        target.write_text(new, encoding="utf-8")
        print("patched pre-open labels + replay panel", target)
    else:
        print("pre-open labels already present", target)


if __name__ == "__main__":
    main()
