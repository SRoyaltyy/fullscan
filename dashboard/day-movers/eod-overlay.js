/* Official EOD overlay for day-movers. */
var NEWS_CACHE = {};
function colorBits(s){
  if(!s) return '';
  return esc(s)
    .replace(/\b(RED|bad|blocked|LAG)\b/g,'<span class="neg">$1</span>')
    .replace(/\b(LEAD|good)\b/g,'<span class="pos">$1</span>')
    .replace(/([+-]\d+(?:\.\d+)?)/g, function(m){ return '<span class="'+(m.charAt(0)==='-'?'neg':'pos')+'">'+m+'</span>'; });
}
function sessionBlock(title, r, official) {
  if (!r) return '<div class="box"><h3>'+esc(title)+'</h3><div class="mut">No prior session in the pack.</div></div>';
  var sit = r.hard_red || (typeof r.s === 'number' && r.s <= -3);
  var o = official && official.o != null ? official.o : r.open;
  var c = official && official.c != null ? official.c : r.close;
  var oc = official && official.oc != null ? official.oc : r.day_pct;
  var src = official && official.px_src ? ' · '+official.px_src : '';
  return '<div class="box"><h3>'+esc(title)+' · '+esc(r.date)+(sit?' <span class="neg">SIT</span>':'')+'</h3>'
    +'<div>'+dots(r.boxes)+'</div><div style="margin-top:6px">'+chips(r.boxes)+'</div>'
    +'<div class="mut" style="margin-top:6px">'+(r.on_list?'list':(r.reconstructed?'recon':'—'))
    +' · cams '+esc(r.cams||('+'+(r.n_pos!=null?r.n_pos:'?')+' −'+(r.n_neg!=null?r.n_neg:'?')))
    +' · idio '+num(r.idio)+' · S '+num(r.s)
    +' · open '+px(o)+' → close '+px(c)+' ('+(oc==null?'—':((oc>=0?'+':'')+Number(oc).toFixed(2)+'%'))+')'+src
    +'</div></div>';
}
function newsLetter(t){
  var ch = String(t||'').charAt(0).toUpperCase();
  return /[A-Z]/.test(ch) ? ch : '0';
}
function isJoinPayload(s){
  if(!s) return true;
  s = String(s).trim();
  if(!s) return true;
  if(/^(join|sector|gen|gen1d|sector1d|ab|peer|cond|lane|blocked)=/i.test(s)) return true;
  if(/;\s*(join|sector|gen|ab|peer|cond)=/i.test(s)) return true;
  return false;
}
function renderNewsList(arts, date, ticker, extraHeadlines){
  var kept = (arts||[]).filter(function(a){
    var ds = a.dates || [];
    if(!ds.length) return String(a.first||a.last||'') <= date;
    return ds.some(function(d){ return d <= date; });
  });
  extraHeadlines = extraHeadlines || [];
  extraHeadlines.forEach(function(h){
    if(!h || !h.title) return;
    var already = kept.some(function(a){
      return (a.title||'') === h.title || (a.digest||'') === h.title;
    });
    if(!already) kept.unshift(h);
  });
  kept.sort(function(a,b){
    var al = a.last || a.first || '';
    var bl = b.last || b.first || '';
    if(al === date && bl !== date) return -1;
    if(bl === date && al !== date) return 1;
    return bl < al ? -1 : bl > al ? 1 : 0;
  });
  if(!kept.length){
    return '<div class="why" id="news-univ"><b>Captured news</b> <span class="mut">no Finviz News Title / Daily Digest on or before '+esc(date)+' for '+esc(ticker)+'.</span></div>';
  }
  var html = '<div class="why" id="news-univ"><b>Captured news</b> <span class="mut">'+kept.length+' unique · on or before '+esc(date)+' · Finviz export universe (not the 400-name digest sample)</span>';
  kept.forEach(function(a){
    var on = (a.dates||[]).indexOf(date) >= 0 || a.last === date || a.first === date;
    var title = a.title || a.digest || '(no title)';
    var extra = a.digest && a.digest !== a.title ? a.digest : '';
    var href = a.url ? ' · <a href="'+esc(a.url)+'" target="_blank" rel="noopener">link</a>' : '';
    var span = (a.first && a.last && a.first!==a.last) ? (a.first+' → '+a.last) : (a.first||a.last||'');
    html += '<div class="news-item'+(on?' on':'')+'">'
      + '<div class="news-kicker">'+(on?'<span class="pos">on session</span>':'<span class="mut">prior</span>')
      + (span?' · '+esc(span):'')+(a.time?' · '+esc(a.time):'')+href+'</div>'
      + '<div class="news-title">'+esc(title)+'</div>'
      + (extra?'<div class="mut">'+esc(extra)+'</div>':'')
      + '</div>';
  });
  html += '</div>';
  return html;
}
function loadTickerNews(ticker, date, extraHeadlines){
  var letter = newsLetter(ticker);
  var apply = function(pack){
    var el = document.getElementById('news-univ');
    if(!el) return;
    el.outerHTML = renderNewsList((pack && pack[ticker]) || [], date, ticker, extraHeadlines);
  };
  if(NEWS_CACHE[letter]){ apply(NEWS_CACHE[letter]); return; }
  var urls = [
    './n/' + letter + '.json?t=' + Date.now(),
    'https://cdn.jsdelivr.net/gh/SRoyaltyy/fullscan@main/dashboard/day-movers/n/' + letter + '.json',
    'https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/dashboard/day-movers/n/' + letter + '.json'
  ];
  var tryAt = function(i, lastErr){
    if(i >= urls.length){
      var el = document.getElementById('news-univ');
      if(!el) return;
      var fallback = renderNewsList([], date, ticker, extraHeadlines);
      if((extraHeadlines||[]).length){ el.outerHTML = fallback; return; }
      el.innerHTML = '<b>Captured news</b> <span class="mut">could not load news pack '+esc(letter)+' — '+esc(lastErr||'missing')+'</span>';
      return;
    }
    fetch(urls[i]).then(function(r){
      if(!r.ok) throw new Error(r.status);
      return r.json();
    }).then(function(pack){
      NEWS_CACHE[letter] = pack || {};
      apply(NEWS_CACHE[letter]);
    }).catch(function(err){ tryAt(i+1, err); });
  };
  tryAt(0);
}
loadDay = function() {
  var date = STATE.dates[STATE.i]; if (!date) return;
  var bundled = (STATE.meta.days || []).find(function(x){ return x.date === date; });
  STATE.day = bundled || { n_open: 0, intradaily: {}, interday: {} };
  STATE.day = bundled || { n_open: 0, intradaily: {}, interday: {} };
  STATE.day = bundled || { n_open: 0, intradaily: {}, interday: {} };
