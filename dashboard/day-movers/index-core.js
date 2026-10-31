const CAM_ORDER = ['join','sector','gen','news','digest','judge','ab','peer','heat','vol','catal','buy','yday'];
const CAM_SHORT = {join:'join',sector:'sect',gen:'gen',news:'news',digest:'dig',judge:'jdg',ab:'AB',peer:'peer',heat:'heat',vol:'vol',catal:'cat',buy:'buy',yday:'yd'};
const STATE = { dates: [], i: 0, side: 'gainers', clock: 'interday', sel: null, meta: null, day: null };
const $ = (id) => document.getElementById(id);
function esc(s){ return String(s??'').replace(/[&<>"]/g,c=>({'&':'&','<':'<','>':'>','"':'"'}[c])); }
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
function span3(r, which){
  if (which==='mx' && r.mx3 != null && isFinite(r.mx3)) return r.mx3;
  if (which==='mn' && r.mn3 != null && isFinite(r.mn3)) return r.mn3;
  const xs = [r.pct, r.h1, r.h3].filter(v => v != null && isFinite(Number(v))).map(Number);
  if (!xs.length) return null;
  return which==='mx' ? Math.max(...xs) : Math.min(...xs);
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
      <td>${pct(span3(r,'mx'))}</td><td>${pct(span3(r,'mn'))}</td>
      <td>${px(r.o)}</td><td>${px(r.x)}</td>
      <td>${num(r.s)}</td><td>${cams(r)}</td><td>${flags(r)}</td>
    </tr>`).join('') || `<tr><td colspan="10">No ${STATE.clock} ${STATE.side} for ${date}.</td></tr>`;
  document.querySelectorAll('#body tr[data-i]').forEach(tr => {
    tr.onclick = () => { STATE.sel = +tr.dataset.i; renderTable(); loadPane(rows[STATE.sel]); };
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
if (!window.CAM_SCREEN) {
fetch('./days.json?t='+Date.now()).then(r => {
  if(!r.ok) throw new Error('days.json '+r.status);
  return r.json();
}).then(d => {
  STATE.meta = d; STATE.dates = d.dates || []; STATE.i = Math.max(0, STATE.dates.length-1); loadDay();
}).catch(err => { $('meta').textContent = 'Failed to load days.json: ' + err; });
}
