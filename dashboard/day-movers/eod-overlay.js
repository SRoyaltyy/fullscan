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
function extrasFromRow(r){
  var out = [];
  if(!r) return out;
  var n = r.news && String(r.news).trim();
  if(n && !isJoinPayload(n)){
    n.split(/\n+/).forEach(function(line){
      line = String(line||'').trim();
      if(line) out.push({title: line, first: r.date, last: r.date, dates: [r.date], src: 'investigator'});
    });
  }
  ['h1','h2','h3','h4','h5'].forEach(function(k){
    var v = r[k+'_headline'] || r[k+'_title'];
    if(v && typeof v === 'string' && v.trim()){
      out.push({title: v.trim(), first: r.date, last: r.date, dates: [r.date], src: 'investigator'});
    }
  });
  return out;
}
function _scanPolarity(text){
  text = String(text||'');
  var pos = [
    /\b(surge[ds]?|soar(?:ed|s|ing)?|jump(?:ed|s)?|rall(?:y|ied)|rebound(?:s|ed|ing)?|lift(?:ing|s|ed)?|record high)\b/i,
    /\bbeats? estimates\b/i,
    /\braises? .{0,24}guidance\b/i,
    /\b(beat|beats|outperform(?:s|ed)?|upgrade[ds]?|overweight|initiated buy|price target raised|bullish)\b/i,
    /\b(approv(?:e[ds]?|al|ing)|fda clearance|cleared|greenlight)\b/i,
    /\b(partnership|collaboration|licen[cs](?:e|ing|ed)|contract win|awarded|backlog|sold out)\b/i,
    /\b(buyback|repurchase|dividend hike|special dividend)\b/i,
    /\b(takeover|acquire[ds]?|acquisition|buyout|go[ -]?private)\b/i,
    /\b(strong (?:q[1-4]|quarter|acv|growth)|growth accelerat)\b/i,
    /\brate cut\b|\beasing\b|\binventory draw\b|\bceasefire\b/i
  ];
  var neg = [
    /\b(plunge[ds]?|crash(?:ed|es)?|slump(?:ed|s)?|selloff|sell-off|bearish)\b/i,
    /\bmisses? estimates\b/i,
    /\bcuts? .{0,24}guidance\b|\bguidance (?:cut|slashed|withdrawn|pulled)\b/i,
    /\b(miss|missed|underperform(?:s|ed)?|downgrade[ds]?|underweight|initiated sell|price target cut)\b/i,
    /\b(lawsuit|sued|probe|investigation|subpoena|fraud|restatement|going concern)\b/i,
    /\b(crl|complete response|rejected|denial|not approv)\b/i,
    /\b(halt(?:ed)?|bankrupt(?:cy)?|default|delist)\b/i,
    /\b(secondary offering|dilutive|atm offering|share offering|warrant exercise)\b/i,
    /\b(layoff|job cut|restructur|resigns? as (?:ceo|cfo)|terminated the)\b/i,
    /\b(rate hike|tightening|inventory build|recession)\b/i,
    /\b(margin pressure|miss on|delay(?:ed|s)? launch|withdraws?|cancelled contract)\b/i,
    /\bshort seller\b/i
  ];
  var neuForce = [
    /\binducement grant/i,
    /\b13f\b/i,
    /\b(presents at|to present|investor (?:conference|day)|what to know|stock of the day|holdings report)\b/i,
    /\bnasdaq listing rule\b/i
  ];
  var hitsP = [], hitsN = [];
  pos.forEach(function(re){ var m = text.match(re); if(m) hitsP.push(m[0].toLowerCase()); });
  neg.forEach(function(re){ var m = text.match(re); if(m) hitsN.push(m[0].toLowerCase()); });
  var forcedNeu = neuForce.some(function(re){ return re.test(text); });
  var pol = 'neutral';
  if(hitsP.length && hitsN.length) pol = 'mixed';
  else if(hitsP.length) pol = '+';
  else if(hitsN.length) pol = '-';
  else if(forcedNeu) pol = 'neutral';
  return {pol: pol, pos: hitsP, neg: hitsN, forcedNeu: forcedNeu};
}
function newsPolarity(title, digest){
  var head = _scanPolarity(title);
  if(head.pol !== 'neutral') return head;
  if(head.forcedNeu) return head;
  var body = _scanPolarity(digest);
  if(body.pol !== 'neutral') return body;
  return head;
}
function polChip(p){
  if(p==='+') return '<span class="pol good">good</span>';
  if(p==='-') return '<span class="pol bad">bad</span>';
  if(p==='mixed') return '<span class="pol mix">mixed</span>';
  return '<span class="pol neu">neutral</span>';
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
  var nPos=0,nNeg=0,nMix=0,nNeu=0,onPos=0,onNeg=0,onMix=0;
  kept.forEach(function(a){
    var scored = newsPolarity(a.title||'', a.digest||'');
    a._pol = scored.pol; a._hitsP = scored.pos; a._hitsN = scored.neg;
    var on = (a.dates||[]).indexOf(date) >= 0 || a.last === date || a.first === date;
    a._on = on;
    if(scored.pol==='+'){ nPos++; if(on) onPos++; }
    else if(scored.pol==='-'){ nNeg++; if(on) onNeg++; }
    else if(scored.pol==='mixed'){ nMix++; if(on) onMix++; }
    else nNeu++;
  });
  var vote = 'session vote ';
  if(onPos||onNeg||onMix) vote += '<span class="pos">+'+onPos+'</span> / <span class="neg">−'+onNeg+'</span>'+(onMix?' / <span class="gold">mix '+onMix+'</span>':'');
  else vote += '<span class="mut">no signed on-session headline</span>';
  var html = '<div class="why" id="news-univ"><b>Captured news</b> <span class="mut">'+kept.length+' unique · on or before '+esc(date)+' · Finviz export universe</span>';
  html += '<div class="news-vote">'+vote+' · all-time in pane <span class="pos">+'+nPos+'</span> / <span class="neg">−'+nNeg+'</span> / mixed '+nMix+' / neu '+nNeu+'</div>';
  if(window.__newsCamMiss && (onPos||onNeg)){
    html += '<div class="news-miss">News camera was '+esc(window.__newsCamMiss)+' at 09:30, but on-session headlines score signed. Catalog polarity was not in the investigator box — that is the miss.</div>';
  }
  kept.forEach(function(a){
    var title = a.title || a.digest || '(no title)';
    var extra = a.digest && a.digest !== a.title ? a.digest : '';
    var href = a.url ? ' · <a href="'+esc(a.url)+'" target="_blank" rel="noopener">link</a>' : '';
    var span = (a.first && a.last && a.first!==a.last) ? (a.first+' → '+a.last) : (a.first||a.last||'');
    var why = [];
    if(a._hitsP && a._hitsP.length) why.push('good: '+a._hitsP.join(', '));
    if(a._hitsN && a._hitsN.length) why.push('bad: '+a._hitsN.join(', '));
    html += '<div class="news-item'+(a._on?' on':'')+' pol-'+ (a._pol==='+'?'good':a._pol==='-'?'bad':a._pol==='mixed'?'mix':'neu') +'">'
      + '<div class="news-kicker">'+(a._on?'<span class="pos">on session</span>':'<span class="mut">prior</span>')
      + ' · '+polChip(a._pol)
      + (span?' · '+esc(span):'')+(a.time?' · '+esc(a.time):'')+href+'</div>'
      + '<div class="news-title">'+esc(title)+'</div>'
      + (extra?'<div class="mut">'+esc(extra)+'</div>':'')
      + (why.length?'<div class="mut">because '+esc(why.join(' · '))+'</div>':'')
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
    'https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/dashboard/day-movers/n/' + letter + '.json'];
  var tryAt = function(i, lastErr){
    if(i >= urls.length){
      var el = document.getElementById('news-univ');
      if(!el) return;
      var fallback = renderNewsList([], date, ticker, extraHeadlines);
      if((extraHeadlines||[]).length){ el.outerHTML = fallback; return; }
      el.innerHTML = '<b>Captured news</b> <span class="mut">could not load news pack '+esc(letter)+' — '+esc(String(lastErr||'missing'))+'</span>';
      return;
    }
    fetch(urls[i]).then(function(r){
      if(!r.ok) throw new Error(urls[i]+' '+r.status);
      return r.json();
    }).then(function(pack){
      NEWS_CACHE[letter] = pack || {};
      apply(NEWS_CACHE[letter]);
    }).catch(function(err){ tryAt(i+1, err); });
  };
  tryAt(0);
}
loadDay = function() {
  if (window.CAM_SCREEN) return;
  var date = STATE.dates[STATE.i]; if (!date) return;
  var bundled = (STATE.meta.days || []).find(function(x){ return x.date === date; });
  STATE.day = bundled || { n_open: 0, intraday: {}, interday: {} };
  STATE.sel = null;
  $('pane').classList.remove('on');
  $('pane').innerHTML = '<div class="empty">click a name</div>';
  renderTable();
  var stamp = Date.now();
  var part = function(sfx){ return fetch('./d/' + date + sfx + '.json?t=' + stamp).then(function(r){ return r.ok ? r.json() : null; }).catch(function(){ return null; }); };
  Promise.all([part(''), part('.ig'), part('.il'), part('.eg'), part('.el')]).then(function(got){
    var over=got[0], ig=got[1], il=got[2], eg=got[3], el=got[4];
    if (STATE.dates[STATE.i] !== date) return;
    var day = over && over.intraday && over.intraday.gainers ? over : null;
    if (!day && ig && ig.rows) {
      var norm = function(pack, clock){
        return (pack.rows || []).map(function(r){
          var o = {};
          for (var k in r) o[k]=r[k];
          if (o.x == null) o.x = clock === 'intraday' ? r.c : r.pc;
          o.k = r.k || (clock === 'intraday' ? 'oc' : 'gap');
          o.px_src = r.px_src || pack.px_src || 'yahoo_eod';
          return o;
        });
      };
      day = {
        date: date, s: ig.s != null ? ig.s : (STATE.day && STATE.day.s), hard_red: !!ig.hard_red,
        n_open: ig.n_open || ig.n, lag: ig.lag || '2026-09-17 OHLC rebased from official Yahoo daily bars.',
        intraday: { n: ig.n || ig.n_open, gainers: norm(ig,'intraday'), losers: il && il.rows ? norm(il,'intraday') : [] },
        interday: { n: (eg && (eg.n || eg.n_open)) || ig.n, gainers: eg && eg.rows ? norm(eg,'interday') : [], losers: el && el.rows ? norm(el,'interday') : [] }
      };
    }
    if (!day) return;
    STATE.day = day;
    if (day.lag) STATE.meta.lag = day.lag;
    renderTable();
  });
};
loadPane = function(listRow) {
  var date = STATE.dates[STATE.i];
  if (!listRow) return;
  var pane = $('pane');
  pane.classList.add('on');
  pane.innerHTML = '<div class="empty">Loading '+esc(listRow.t)+' '+esc(date)+'…</div>';
  var href = '../hard-red-exceptions/?ticker=' + encodeURIComponent(listRow.t) + '#' + encodeURIComponent(listRow.t);
  fetch('../hard-red-exceptions/t/' + encodeURIComponent(listRow.t) + '.json').then(function(res){
    if (!res.ok) throw new Error(res.status);
    return res.json();
  }).then(function(doc){
    var rows = doc.rows || [];
    var idx = rows.findIndex(function(x){ return x.date === date; });
    var r = idx >= 0 ? rows[idx] : null;
    var prev = idx > 0 ? rows[idx-1] : null;
    if (!r) { pane.innerHTML = '<div class="empty">No investigator row for '+esc(listRow.t)+' on '+esc(date)+'.</div>'; return; }
    var good = litList(r.boxes, 'good'), bad = litList(r.boxes, 'bad');
    var neu = litList(r.boxes, 'neutral'), miss = litList(r.boxes, 'missing');
    var changed = camDiff(r.boxes, prev && prev.boxes);
    var legal = STATE.clock === 'interday' && listRow.gap != null;
    var caught = r.news && String(r.news).trim();
    var invDrift = (listRow.o != null && r.open != null && Math.abs(Number(listRow.o)-Number(r.open)) > 0.02)
                || (listRow.c != null && r.close != null && Math.abs(Number(listRow.c)-Number(r.close)) > 0.02);
    pane.innerHTML = '<h2>'+esc(doc.ticker)+' <span class="sub">'+esc(date)+' 09:30</span> '+setupBadge(listRow.su, listRow.sq)+'</h2>'
      +'<div class="mut">'+(r.on_list?'on shopping list':'reconstructed — not on the lists')+' · '+flags(listRow)+' · <a href="'+href+'">full history</a></div>'
      +'<div class="kv">'
      +'<div>Move intra / inter</div><b>'+pct(listRow.oc)+' / '+pct(listRow.gap)+'</b>'
      +'<div>Official OHLC</div><b>'+px(listRow.o)+' / '+px(listRow.h)+' / '+px(listRow.l)+' / '+px(listRow.c)+(listRow.px_src?' <span class="mut">('+esc(listRow.px_src)+')</span>':'')+'</b>'
      +'<div>Open → close</div><b>'+px(listRow.o)+' → '+px(listRow.c)+' ('+pct(listRow.oc)+')</b>'
      +'<div>Prior close / gap</div><b>'+px(listRow.pc)+' · '+pct(listRow.gap)+'</b>'
      +'<div>Weather S</div><b>'+num(r.s)+(r.hard_red?' · SIT':'')+'</b>'
      +'<div>Idio / lean</div><b>'+num(r.idio)+'</b>'
      +'<div>Src</div><b>'+esc((listRow.src||r.sources||[]).join(', ')||'—')+'</b>'
      +'<div>Legal at 09:30?</div><b>'+(legal?'yes — interday gap / prior close / open':'no — close print or missing prior')+'</b>'
      +'</div>'
      +(invDrift?'<div class="mut">Investigator print was '+px(r.open)+' → '+px(r.close)+' — ignored; board uses official EOD.</div>':'')
      +'<div class="cmp">'+sessionBlock('Today', r, listRow)+sessionBlock('Yesterday', prev)+'</div>'
      +'<div class="why"><b>What lit today</b>'
      +'<div>good: '+(good.length?good.map(esc).join(', '):'—')+'</div>'
      +'<div>bad: '+(bad.length?bad.map(esc).join(', '):'—')+'</div>'
      +'<div>neutral: '+(neu.length?neu.map(esc).join(', '):'—')+'</div>'
      +'<div class="mut">missing: '+(miss.length?miss.map(esc).join(', '):'—')+'</div>'
      +'<div style="margin-top:8px"><b>vs yesterday</b> '+(changed.length?('<ul>'+changed.map(function(x){return '<li>'+esc(x)+'</li>';}).join('')+'</ul>'):'<div class="mut">no prior row, or every camera unchanged</div>')+'</div></div>'
      +'<div class="caught"><b>Caught text</b> '+(r.news_tone && r.news_tone!=='missing' ? '('+esc(r.news_tone)+')' : '<span class="mut">(payload — not a headline)</span>')+'<br>'
      +(caught?colorBits(caught):'<span class="mut">No headline / payload stored. News camera is '+(r.boxes&&r.boxes.news?esc(r.boxes.news):'missing')+'.</span>')+'</div>'
      +'<div class="why"><b>Why / files</b>'
      +'<div class="mut">files: '+esc((r.files||[]).join(' · ')||'—')+'</div>'
      +'<div class="mut">sources: '+esc((r.sources||[]).join(', ')||'—')+'</div>'
      +'<ul>'+(r.why||[]).map(function(x){return '<li>'+esc(x)+'</li>';}).join('')+'</ul></div>'
      +'<div class="why" id="news-univ"><b>Captured news</b> <span class="mut">loading…</span></div>';
    window.__newsCamMiss = (r.boxes && r.boxes.news && r.boxes.news !== 'good' && r.boxes.news !== 'bad') ? r.boxes.news : ((r.boxes && (r.boxes.news==='good'||r.boxes.news==='bad')) ? '' : 'missing');
    loadTickerNews(doc.ticker || listRow.t, date, extrasFromRow(r));
  }).catch(function(err){
    pane.innerHTML = '<h2>'+esc(listRow.t)+' <span class="sub">'+esc(date)+'</span></h2><div class="kv"><div>Move</div><b>'+pct(listRow.pct)+'</b><div>OHLC</div><b>'+px(listRow.o)+' / '+px(listRow.h)+' / '+px(listRow.l)+' / '+px(listRow.c)+'</b></div><div class="empty">Could not load investigator file.<br>'+esc(err)+'</div>';
  });
};
if (STATE.dates && STATE.dates.length) loadDay();
