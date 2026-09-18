/* +5 / -2 camera screen — packed per date under cam/, pane reused from eod-overlay.js */
STATE.camDays = {};
STATE.camMeta = null;

function camAsRow(r) {
  const intra = STATE.clock === 'intraday';
  return {
    t: r.t,
    pct: intra ? r.pct_i : r.pct_e,
    o: r.o, c: r.c, pc: r.pc,
    x: intra ? r.c : r.pc,
    h: r.h, l: r.l,
    s: r.s, hr: r.hr, np: r.np, nn: r.nn,
    idio: r.idio,
    mx3: intra ? r.mx3 : r.mx3g,
    mn3: intra ? r.mn3 : r.mn3g,
    n3: r.n3,
    h1: r.h1, h3: r.h3,
    hot4: r.hot4, fl: r.fl, su: r.su, sq: r.sq,
    oc: r.pct_i, gap: r.pct_e,
    src: r.src || [],
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
  $('date').textContent = date;
  const rows = rowsOf();
  const nDay = (STATE.camDays[date] || []).length;
  $('meta').textContent = `${d.n_tickers_scanned || '—'} histories · ${d.from_date} → ${d.to_date} · filter +cams≥5 and −cams≤2 · ${nDay} names this session · showing ${rows.length} with ${STATE.clock} print · ${STATE.side}`;
  const banner = $('banner');
  if (STATE.clock === 'intraday') {
    banner.className = 'banner show';
    banner.textContent = 'same-session close — not knowable at 09:30. 3d max/min = max/min close over today + next 2 sessions vs today’s open.';
  } else {
    banner.className = 'banner show';
    banner.textContent = 'Interday = prior close → today’s open. 3d max/min = max/min close over today + next 2 sessions vs prior close.';
  }
  const lag = $('lag');
  if (d.lag) { lag.className = 'banner show lag'; lag.textContent = d.lag; }
  else { lag.className = 'banner'; lag.textContent = ''; }
  $('pxHead').textContent = STATE.clock === 'intraday' ? 'Close' : 'Prior close';
  $('body').innerHTML = rows.map((r, i) => `
    <tr data-i="${i}" class="${STATE.sel===i?'sel':''}">
      <td>${i+1}</td><td><strong>${esc(r.t)}</strong></td><td>${pct(r.pct)}</td>
      <td>${pct(r.mx3)}</td><td>${pct(r.mn3)}</td>
      <td>${px(r.o)}</td><td>${px(r.x)}</td>
      <td>${num(r.s)}</td><td>${cams(r)}</td><td>${flags(r)}</td>
    </tr>`).join('') || `<tr><td colspan="10">No +5/−2 names with a ${STATE.clock} print on ${date}.</td></tr>`;
  document.querySelectorAll('#body tr[data-i]').forEach(tr => {
    tr.onclick = () => { STATE.sel = +tr.dataset.i; renderTable(); loadPane(rows[STATE.sel]); };
  });
};

loadDay = function () {
  const date = STATE.dates[STATE.i];
  if (!date) return;
  STATE.sel = null;
  $('pane').classList.remove('on');
  $('pane').innerHTML = '<div class="empty">click a name</div>';
  if (STATE.camDays[date]) {
    STATE.day = { date, n_open: STATE.camDays[date].length };
    renderTable();
    return;
  }
  $('body').innerHTML = `<tr><td colspan="10">Loading ${esc(date)}…</td></tr>`;
  fetch('./cam/' + date + '.json?t=' + Date.now()).then(r => {
    if (!r.ok) throw new Error(date + ' ' + r.status);
    return r.json();
  }).then(day => {
    if (STATE.dates[STATE.i] !== date) return;
    STATE.camDays[date] = day.rows || [];
    STATE.day = { date, n_open: STATE.camDays[date].length, s: day.s };
    renderTable();
  }).catch(err => {
    if (STATE.dates[STATE.i] !== date) return;
    $('meta').textContent = 'Failed to load cam/' + date + '.json: ' + err;
  });
};

fetch('./cam-index.json?t=' + Date.now()).then(r => {
  if (!r.ok) throw new Error('cam-index.json ' + r.status);
  return r.json();
}).then(d => {
  STATE.camMeta = d;
  STATE.meta = {
    from_date: d.from_date,
    to_date: d.to_date,
    n_tickers_scanned: d.n_tickers_scanned,
    n_tickers: d.n_tickers_scanned,
    top_n: d.n_rows,
    lag: null,
    dates: d.dates || [],
  };
  STATE.dates = d.dates || [];
  STATE.camDays = {};
  STATE.i = Math.max(0, STATE.dates.length - 1);
  loadDay();
}).catch(err => { $('meta').textContent = 'Failed to load cam-index.json: ' + err; });
