const CAM_ORDER = ['join','sector','gen','news','digest','judge','ab','peer','heat','vol','catal','buy','yday'];
const CAM_SHORT = {join:'join',sector:'sect',gen:'gen',news:'news',digest:'dig',judge:'jdg',ab:'AB',peer:'peer',heat:'heat',vol:'vol',catal:'cat',buy:'buy',yday:'yd'};
const STATE = { dates: [], i: 0, side: 'gainers', clock: 'interday', sel: null, meta: null, day: null };
const $ = (id) => document.getElementById(id);
function esc(s){ return String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }
function pct(v) { if (v == null || !isFinite(v)) return '—'; const s = (v >= 0 ? '+' : '') + Number(v).toFixed(2) + '%'; return `<span class="${v>=0?'up':'dn'}">${s}</span>`; }
function num(v) { return v == null || !isFinite(Number(v)) ? '—' : v; }
function px(v) { return v == null || !isFinite(Number(v)) ? '—' : Number(v).toFixed(2); }
function dots(boxes) {
  return CAM_ORDER.map(k => { const t = String((boxes||{})[k] || 'missing'); return `<span class="dot ${esc(t)}" title="${esc(k)} ${esc(t)}"></span>`; }).join('');
}
function chips(boxes) {
  return CAM_ORDER.map(k => { const t = String((boxes||{})[k] || 'missing'); const cls = t==='good'?'pos':t==='bad'?'neg':t==='neutral'?'neu':'miss'; return `<span class="cam ${cls}">${esc(CAM_SHORT[k]||k)} ${esc(t)}</span>`; }).join('');
}
function litList(boxes, want) { return CAM_ORDER.filter(k => (boxes||{})[k] === want).map(k => CAM_SHORT[k]||k); }
function camDiff(today, yday) {
  const t = today || {}, y = yday || {}, changed = [];
  for (const k of CAM_ORDER) { const a = t[k] || 'missing', b = y[k] || 'missing'; if (a !== b) changed.push(`${CAM_SHORT[k]||k}: ${b} → ${a}`); }
  return changed;
}
function setupLogo(side){
  if(side==='long'){
    return '<svg class="logo" viewBox="0 0 12 12" aria-hidden="true"><circle cx="6" cy="6" r="5.5" fill="#14532d"/><path d="M6 3.1 8.7 8H3.3Z" fill="#3ddc97"/></svg>';
  }
  return '<svg class="logo" viewBox="0 0 12 12" aria-hidden="true"><circle cx="6" cy="6" r="5.5" fill="#7f1d1d"/><path d="M6 8.9 3.3 4h5.4Z" fill="#ff6b7a"/></svg>';
}
function setupBadge(su, sq){
  if(!su) return '';
  const side=su==='s'?'short':'long';
  const explore=sq==='e';
  const mark=explore?' <span title="explore">?</span>':'';
  return `<span class="setup ${side}${explore?' explore':''}">${setupLogo(side)} ${side==='long'?'LONG':'SHORT'}${mark}</span>`;
}
function flags(r){
  let s='';
  if(r.hot4) s+='<span class="flag hot">hot4</span>';
  if(r.fl) s+='<span class="flag fl">flatten would-buy</span>';
  s+=setupBadge(r.su, r.sq);
  return s||'<span class="mut">—</span>';
}
function cams(r){
  if(r.np==null && r.nn==null) return '<span class="mut">—</span>';
  return `<span class="pos">+${esc(r.np||0)}</span> <span class="neg">−${esc(r.nn||0)}</span>`;
}
function rowsOf(){
  const day = STATE.day || {};
  return ((day[STATE.clock] || {})[STATE.side]) || [];
}
function renderTable() {
  const d = STATE.meta, day = STATE.day || {};
  if (!d) return;
  const date = STATE.dates[STATE.i];
  $('date').textContent = date;
  const rows = rowsOf();
  const pack = day[STATE.clock] || {};
  $('meta').textContent = `${d.n_tickers || d.n_tickers_scanned || '—'} histories · ${d.from_date} → ${d.to_date} · universe ${pack.n ?? day.n_open ?? 0} · S=${day.s ?? '—'} · top ${d.top_n || 25} · ${STATE.side} · ${STATE.clock}`;
  const banner = $('banner');
  if (STATE.clock === 'intraday') {
    banner.className = 'banner show';
    banner.textContent = 'same-session close — not knowable at 09:30.';
  } else {
    banner.className = 'banner show';
    banner.textContent = 'Interday = prior close → today’s 09:30 open. Legal at the bell. Never ranked by same-day Change%.';
  }
  const lag = $('lag');
  if (d.lag) { lag.className = 'banner show lag'; lag.textContent = d.lag; }
  else { lag.className = 'banner'; lag.textContent = ''; }
  $('pxHead').textContent = STATE.clock === 'intraday' ? 'Close' : 'Prior close';
  $('body').innerHTML = rows.map((r, i) => `
    <tr data-i="${i}" class="${STATE.sel===i?'sel':''}">
      <td>${i+1}</td><td><strong>${esc(r.t)}</strong></td><td>${pct(r.pct)}</td>
      <td>${px(r.o)}</td><td>${px(r.x)}</td>
      <td>${num(r.s)}</td><td>${cams(r)}</td><td>${flags(r)}</td>
    </tr>`).join('') || `<tr><td colspan="8">No ${STATE.clock} ${STATE.side} for ${date}.</td></tr>`;
  document.querySelectorAll('#body tr[data-i]').forEach(tr => {
    tr.onclick = () => { STATE.sel = +tr.dataset.i; renderTable(); loadPane(rows[STATE.sel]); };
  });
}
function sessionBlock(title, r) {
  if (!r) return `<div class="box"><h3>${esc(title)}</h3><div class="mut">No prior session in the pack.</div></div>`;
  const sit = r.hard_red || (typeof r.s === 'number' && r.s <= -3);
  return `<div class="box">
    <h3>${esc(title)} · ${esc(r.date)}${sit?' <span class="neg">SIT</span>':''}</h3>
    <div>${dots(r.boxes)}</div>
    <div style="margin-top:6px">${chips(r.boxes)}</div>
    <div class="mut" style="margin-top:6px">
      ${r.on_list?'list':(r.reconstructed?'recon':'—')}
      · cams ${esc(r.cams||('+'+(r.n_pos??'?')+' −'+(r.n_neg??'?')))}
      · idio ${num(r.idio)} · S ${num(r.s)}
      · open ${px(r.open)} → close ${px(r.close)} (${r.day_pct==null?'—':((r.day_pct>=0?'+':'')+Number(r.day_pct).toFixed(2)+'%')})
    </div></div>`;
}
function loadPane(listRow) {
  const date = STATE.dates[STATE.i];
  if (!listRow) return;
  const pane = $('pane');
  pane.classList.add('on');
  pane.innerHTML = `<div class="empty">Loading ${esc(listRow.t)} ${esc(date)}…</div>`;
  const href = '../hard-red-exceptions/?ticker=' + encodeURIComponent(listRow.t) + '#' + encodeURIComponent(listRow.t);
  fetch('../hard-red-exceptions/t/' + encodeURIComponent(listRow.t) + '.json').then(r => {
    if (!r.ok) throw new Error(r.status);
    return r.json();
  }).then(doc => {
    const rows = doc.rows || [];
    const idx = rows.findIndex(x => x.date === date);
    const r = idx >= 0 ? rows[idx] : null;
    const prev = idx > 0 ? rows[idx-1] : null;
    if (!r) { pane.innerHTML = `<div class="empty">No investigator row for ${esc(listRow.t)} on ${esc(date)}.</div>`; return; }
    const good = litList(r.boxes, 'good'), bad = litList(r.boxes, 'bad');
    const neu = litList(r.boxes, 'neutral'), miss = litList(r.boxes, 'missing');
    const changed = camDiff(r.boxes, prev && prev.boxes);
    const legal = STATE.clock === 'interday' && listRow.gap != null;
    const caught = r.news && String(r.news).trim();
    pane.innerHTML = `
      <h2>${esc(doc.ticker)} <span class="sub">${esc(date)} 09:30</span> ${setupBadge(listRow.su, listRow.sq)}</h2>
      <div class="mut">${r.on_list?'on shopping list':'reconstructed — not on the lists'}
        · ${flags(listRow)}
        · <a href="${href}">full history</a></div>
      <div class="kv">
        <div>Move intra / inter</div><b>${listRow.oc==null?'—':((listRow.oc>=0?'+':'')+Number(listRow.oc).toFixed(2)+'%')} / ${listRow.gap==null?'—':((listRow.gap>=0?'+':'')+Number(listRow.gap).toFixed(2)+'%')}</b>
        <div>OHLC</div><b>${px(listRow.o||r.open)} / ${px(listRow.h)} / ${px(listRow.l)} / ${px(listRow.c||r.close)}</b>
        <div>Open → close</div><b>${px(r.open)} → ${px(r.close)} (${r.day_pct==null?'—':((r.day_pct>=0?'+':'')+Number(r.day_pct).toFixed(2)+'%')})</b>
        <div>Prior close / gap</div><b>${px(listRow.pc)} · ${listRow.gap==null?'—':((listRow.gap>=0?'+':'')+Number(listRow.gap).toFixed(2)+'%')}</b>
        <div>Weather S</div><b>${num(r.s)}${r.hard_red?' · SIT':''}</b>
        <div>Idio / lean</div><b>${num(r.idio)}</b>
        <div>Src</div><b>${esc((listRow.src||r.sources||[]).join(', ')||'—')}</b>
        <div>hot4 / flatten</div><b>${listRow.hot4?'yes — union_hot_n4_h1':'no / soft-miss'} · ${listRow.fl?'yes — flatten_robust would-buy':'no / soft-miss'}</b>
        <div>H1 / H3 / H5</div><b>${r.h1==null?'—':((r.h1>=0?'+':'')+Number(r.h1).toFixed(2)+'%')} ${r.h1_px!=null?'@'+px(r.h1_px):''} · ${r.h3==null?'—':((r.h3>=0?'+':'')+Number(r.h3).toFixed(2)+'%')} · ${r.h5==null?'—':((r.h5>=0?'+':'')+Number(r.h5).toFixed(2)+'%')}</b>
        <div>E / R</div><b>${esc(r.e_label||r.e_pol||'—')} / ${esc(r.r_label||r.r_pol||'—')}</b>
        <div>RSI / MACD</div><b>${esc(r.rsi??'—')} / ${esc(r.macd_hist??'—')}</b>
        <div>Legal at 09:30?</div><b>${legal?'yes — interday gap / prior close / open':'no — close print or missing prior'}</b>
      </div>
      <div class="cmp">${sessionBlock('Today', r)}${sessionBlock('Yesterday', prev)}</div>
      <div class="why">
        <b>What lit today</b>
        <div>good: ${good.length?good.map(esc).join(', '):'—'}</div>
        <div>bad: ${bad.length?bad.map(esc).join(', '):'—'}</div>
        <div>neutral: ${neu.length?neu.map(esc).join(', '):'—'}</div>
        <div class="mut">missing: ${miss.length?miss.map(esc).join(', '):'—'}</div>
        <div style="margin-top:8px"><b>vs yesterday</b> ${changed.length?('<ul>'+changed.map(x=>'<li>'+esc(x)+'</li>').join('')+'</ul>'):'<div class="mut">no prior row, or every camera unchanged</div>'}</div>
      </div>
      <div class="caught"><b>Caught text</b> ${r.news_tone && r.news_tone!=='missing' ? '('+esc(r.news_tone)+')' : ''}<br>${caught?esc(caught):'<span class="mut">No headline / payload stored. News camera is '+(r.boxes&&r.boxes.news?esc(r.boxes.news):'missing')+'.</span>'}</div>
      <div class="why"><b>Why / files</b>
        <div class="mut">files: ${esc((r.files||[]).join(' · ')||'—')}</div>
        <div class="mut">sources: ${esc((r.sources||[]).join(', ')||'—')}</div>
        <ul>${(r.why||[]).map(x=>'<li>'+esc(x)+'</li>').join('')}</ul>
      </div>`;
  }).catch(err => {
    pane.innerHTML = `<h2>${esc(listRow.t)} <span class="sub">${esc(date)}</span></h2>
      <div class="kv">
        <div>Move</div><b>${pct(listRow.pct)}</b>
        <div>OHLC</div><b>${px(listRow.o)} / ${px(listRow.h)} / ${px(listRow.l)} / ${px(listRow.c)}</b>
        <div>Gap / o→c</div><b>${pct(listRow.gap)} / ${pct(listRow.oc)}</b>
        <div>S / sit</div><b>${num(listRow.s)}${listRow.hr?' · SIT':''}</b>
        <div>Legal at 09:30?</div><b>${STATE.clock==='interday'?'yes — interday fields':'no — same-session close'}</b>
      </div>
      <div class="empty">Could not load investigator file for ${esc(listRow.t)}.<br>${esc(err)}</div>`;
  });
}
function bindTog(id, field) {
  $(id).querySelectorAll('button[data-v]').forEach(btn => {
    btn.onclick = () => {
      $(id).querySelectorAll('button[data-v]').forEach(b => b.classList.toggle('on', b===btn));
      STATE[field] = btn.dataset.v; STATE.sel = null;
      $('pane').classList.remove('on');
      $('pane').innerHTML = '<div class="empty">click a name</div>';
      renderTable();
    };
  });
}
bindTog('side', 'side'); bindTog('clock', 'clock');
function loadDay() {
  const date = STATE.dates[STATE.i]; if (!date) return;
  const day = (STATE.meta.days || []).find(x => x.date === date);
  STATE.day = day || { n_open: 0, intraday: {}, interday: {} };
  STATE.sel = null;
  $('pane').classList.remove('on');
  $('pane').innerHTML = '<div class="empty">click a name</div>';
  renderTable();
}
$('first').onclick = () => { STATE.i = 0; loadDay(); };
$('last').onclick = () => { STATE.i = STATE.dates.length-1; loadDay(); };
$('prev').onclick = () => { STATE.i = Math.max(0, STATE.i-1); loadDay(); };
$('next').onclick = () => { STATE.i = Math.min(STATE.dates.length-1, STATE.i+1); loadDay(); };
fetch('./days.json?t='+Date.now()).then(r => {
  if(!r.ok) throw new Error('days.json '+r.status);
  return r.json();
}).then(d => {
  STATE.meta = d; STATE.dates = d.dates || []; STATE.i = Math.max(0, STATE.dates.length-1); loadDay();
}).catch(err => { $('meta').textContent = 'Failed to load days.json: ' + err; });
