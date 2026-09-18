/* Official EOD overlay for day-movers. */
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
loadDay = function() {
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
      +'<ul>'+(r.why||[]).map(function(x){return '<li>'+esc(x)+'</li>';}).join('')+'</ul></div>';
  }).catch(function(err){
    pane.innerHTML = '<h2>'+esc(listRow.t)+' <span class="sub">'+esc(date)+'</span></h2><div class="kv"><div>Move</div><b>'+pct(listRow.pct)+'</b><div>OHLC</div><b>'+px(listRow.o)+' / '+px(listRow.h)+' / '+px(listRow.l)+' / '+px(listRow.c)+'</b></div><div class="empty">Could not load investigator file.<br>'+esc(err)+'</div>';
  });
};
if (STATE.dates && STATE.dates.length) loadDay();
