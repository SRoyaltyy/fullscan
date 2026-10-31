/* +5 / −2 camera screen — load compact pack first, then per-day shards. */
STATE.camDays = {};
STATE.camMeta = null;

function camAsRow(r) {
  const intra = STATE.clock === 'intraday';
  const pctI = r.pct_i != null ? r.pct_i : r.i;
  const pctE = r.pct_e != null ? r.pct_e : r.e;
  const mx = intra ? (r.mx3 != null ? r.mx3 : r.mx) : (r.mx3g != null ? r.mx3g : r.mxg);
  const mn = intra ? (r.mn3 != null ? r.mn3 : r.mn) : (r.mn3g != null ? r.mn3g : r.mng);
  return {
    t: r.t, pct: intra ? pctI : pctE,
    o: r.o, c: r.c, pc: r.pc, x: intra ? r.c : r.pc,
    s: r.s, hr: !!(r.hr), np: r.np, nn: r.nn, idio: r.idio,
    mx3: mx, mn3: mn, h1: r.h1, h3: r.h3, su: r.su, sq: r.sq,
    oc: pctI, gap: pctE
  };
}

rowsOf = function () {
  const date = STATE.dates[STATE.i];
  const raw = (STATE.camDays[date] || []);
  const rows = raw.map(camAsRow).filter(r => r.pct != null && isFinite(r.pct));
  rows.sort((a, b) => STATE.side === 'gainers' ? (b.pct - a.pct) : (a.pct - b.pct));
  return rows;
};

renderTable = function () {
  const d = STATE.meta;
  if (!d) return;
  const date = STATE.dates[STATE.i];
  $('date').textContent = date || '—';
  const rows = rowsOf();
  const nDay = (STATE.camDays[date] || []).length;
  $('meta').textContent = (d.n_tickers_scanned || '—') + ' histories · ' + (d.from_date||'') + ' → ' + (d.to_date||'') +
    ' · +cams≥5 −cams≤2 · ' + nDay + ' names · ' + rows.length + ' with ' + STATE.clock + ' print · ' + STATE.side;
  const banner = $('banner');
  banner.className = 'banner show';
  banner.textContent = STATE.clock === 'intraday'
    ? 'same-session close — not knowable at 09:30. 3d max/min vs today’s open.'
    : 'Interday = prior close → today’s open. 3d max/min vs prior close.';
  $('pxHead').textContent = STATE.clock === 'intraday' ? 'Close' : 'Prior close';
  $('body').innerHTML = rows.map((r, i) =>
    '<tr data-i="' + i + '" class="' + (STATE.sel===i?'sel':'') + '">' +
      '<td>'+(i+1)+'</td><td><strong>'+esc(r.t)+'</strong></td><td>'+pct(r.pct)+'</td>' +
      '<td>'+pct(r.mx3)+'</td><td>'+pct(r.mn3)+'</td>' +
      '<td>'+px(r.o)+'</td><td>'+px(r.x)+'</td>' +
      '<td>'+num(r.s)+'</td><td>'+cams(r)+'</td><td>'+flags(r)+'</td></tr>'
  ).join('') || '<tr><td colspan="10">No +5/−2 names with a '+STATE.clock+' print on '+date+'.</td></tr>';
  document.querySelectorAll('#body tr[data-i]').forEach(tr => {
    tr.onclick = () => { STATE.sel = +tr.dataset.i; renderTable(); loadPane(rows[STATE.sel]); };
  });
};

function fetchJson(url, ms) {
  const ctrl = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const timer = ctrl ? setTimeout(function(){ ctrl.abort(); }, ms || 8000) : null;
  return fetch(url, ctrl ? { signal: ctrl.signal } : {}).then(function(r){
    if (!r.ok) throw new Error(String(r.status));
    return r.json();
  }).finally(function(){ if (timer) clearTimeout(timer); });
}

function tryUrls(urls, i, ok, fail, lastErr) {
  if (i >= urls.length) { fail(lastErr || 'missing'); return; }
  fetchJson(urls[i], 8000).then(ok).catch(function(err){ tryUrls(urls, i + 1, ok, fail, err); });
}

function applyPack(d) {
  STATE.camMeta = d;
  STATE.meta = {
    from_date: d.from_date, to_date: d.to_date,
    n_tickers_scanned: d.n_tickers_scanned, n_tickers: d.n_tickers_scanned,
    top_n: d.n_rows, lag: null, dates: d.dates || []
  };
  STATE.dates = d.dates || [];
  (d.days || []).forEach(function(day){
    if (day && day.date) STATE.camDays[day.date] = day.rows || [];
  });
  if (d.by_date) {
    Object.keys(d.by_date).forEach(function(dt){
      STATE.camDays[dt] = d.by_date[dt].rows || d.by_date[dt] || [];
    });
  }
  STATE.i = Math.max(0, STATE.dates.length - 1);
  loadDay();
}

function dateUrls(date) {
  const t = Date.now();
  const name = 'cam/' + date + '.json';
  return [
    './' + name + '?t=' + t,
    'https://cdn.jsdelivr.net/gh/SRoyaltyy/fullscan@main/dashboard/day-movers/' + name,
    'https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/dashboard/day-movers/' + name
  ];
}

loadDay = function () {
  const date = STATE.dates[STATE.i];
  if (!date) return;
  STATE.sel = null;
  $('pane').classList.remove('on');
  $('pane').innerHTML = '<div class="empty">click a name</div>';
  if (STATE.camDays[date]) {
    STATE.day = { date: date, n_open: STATE.camDays[date].length };
    renderTable();
    return;
  }
  $('body').innerHTML = '<tr><td colspan="10">Loading '+esc(date)+'…</td></tr>';
  tryUrls(dateUrls(date), 0, function(day) {
    if (STATE.dates[STATE.i] !== date) return;
    STATE.camDays[date] = day.rows || [];
    STATE.day = { date: date, n_open: STATE.camDays[date].length, s: day.s };
    renderTable();
  }, function(err) {
    if (STATE.dates[STATE.i] !== date) return;
    $('meta').textContent = 'Failed to load ' + date + ': ' + err;
    $('body').innerHTML = '<tr><td colspan="10">Failed to load '+esc(date)+' — '+esc(String(err))+'</td></tr>';
  });
};

function boot() {
  const t = Date.now();
  $('meta').textContent = 'Loading cam pack…';
  tryUrls([
    './cam.compact.json?t=' + t,
    'https://cdn.jsdelivr.net/gh/SRoyaltyy/fullscan@main/dashboard/day-movers/cam.compact.json',
    'https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/dashboard/day-movers/cam.compact.json',
    './cam-index.json?t=' + t,
    'https://cdn.jsdelivr.net/gh/SRoyaltyy/fullscan@main/dashboard/day-movers/cam-index.json',
    'https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/dashboard/day-movers/cam-index.json'
  ], 0, applyPack, function(err) {
    $('meta').textContent = 'Failed to load cam pack: ' + err;
    $('body').innerHTML = '<tr><td colspan="10">Failed to load cam pack — '+esc(String(err))+'</td></tr>';
  });
}
boot();
