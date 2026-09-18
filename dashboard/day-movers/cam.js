/* +5 / -2 camera screen. Loads compact cam-aug.json + cam-sep.json (local / raw / jsdelivr). */
STATE.camDays = {};
STATE.camMeta = null;

function camAsRow(r) {
  const intra = STATE.clock === 'intraday';
  const pctI = r.pct_i != null ? r.pct_i : r.i;
  const pctE = r.pct_e != null ? r.pct_e : r.e;
  const mx = intra ? (r.mx3 != null ? r.mx3 : r.mx) : (r.mx3g != null ? r.mx3g : r.mxg);
  const mn = intra ? (r.mn3 != null ? r.mn3 : r.mn) : (r.mn3g != null ? r.mn3g : r.mng);
  return {
    t: r.t,
    pct: intra ? pctI : pctE,
    o: r.o, c: r.c, pc: r.pc,
    x: intra ? r.c : r.pc,
    s: r.s, hr: !!(r.hr), np: r.np, nn: r.nn,
    idio: r.idio,
    mx3: mx, mn3: mn,
    h1: r.h1, h3: r.h3,
    su: r.su, sq: r.sq,
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
  $('date').textContent = date;
  const rows = rowsOf();
  const nDay = (STATE.camDays[date] || []).length;
  $('meta').textContent = (d.n_tickers_scanned || '—') + ' histories · ' + d.from_date + ' → ' + d.to_date +
    ' · filter +cams≥5 and −cams≤2 · ' + nDay + ' names this session · showing ' + rows.length +
    ' with ' + STATE.clock + ' print · ' + STATE.side;
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
  $('body').innerHTML = rows.map((r, i) =>
    '<tr data-i="' + i + '" class="' + (STATE.sel===i?'sel':'') + '">' +
      '<td>' + (i+1) + '</td><td><strong>' + esc(r.t) + '</strong></td><td>' + pct(r.pct) + '</td>' +
      '<td>' + pct(r.mx3) + '</td><td>' + pct(r.mn3) + '</td>' +
      '<td>' + px(r.o) + '</td><td>' + px(r.x) + '</td>' +
      '<td>' + num(r.s) + '</td><td>' + cams(r) + '</td><td>' + flags(r) + '</td>' +
    '</tr>'
  ).join('') || '<tr><td colspan="10">No +5/−2 names with a ' + STATE.clock + ' print on ' + date + '.</td></tr>';
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
  STATE.day = { date: date, n_open: (STATE.camDays[date] || []).length };
  renderTable();
};

function tryUrls(urls, i, ok, fail, lastErr) {
  if (i >= urls.length) { fail(lastErr || 'missing'); return; }
  fetch(urls[i]).then(r => {
    if (!r.ok) throw new Error(urls[i] + ' ' + r.status);
    return r.json();
  }).then(ok).catch(err => tryUrls(urls, i+1, ok, fail, err));
}

function packUrls(name) {
  const t = Date.now();
  return [
    './' + name + '?t=' + t,
    'https://raw.githubusercontent.com/SRoyaltyy/fullscan/main/dashboard/day-movers/' + name,
    'https://cdn.jsdelivr.net/gh/SRoyaltyy/fullscan@main/dashboard/day-movers/' + name,
    'https://raw.githubusercontent.com/SRoyaltyy/fullscan/gh-pages/dashboard/day-movers/' + name
  ];
}

function ingest(pack) {
  if (!pack) return;
  (pack.days || []).forEach(day => {
    STATE.camDays[day.date] = day.rows || [];
  });
  const dates = Object.keys(STATE.camDays).sort();
  STATE.dates = dates;
  STATE.meta = {
    from_date: dates[0],
    to_date: dates[dates.length-1],
    n_tickers_scanned: (pack.n_tickers_scanned || STATE.meta && STATE.meta.n_tickers_scanned || 1249),
    n_tickers: pack.n_tickers_scanned || 1249,
    top_n: pack.n_rows,
    lag: null,
    dates: dates
  };
}

function boot() {
  $('meta').textContent = 'Loading cam packs…';
  let left = 2, errA = null, errB = null;
  function done() {
    left -= 1;
    if (left > 0) return;
    if (!STATE.dates.length) {
      $('meta').textContent = 'Failed to load cam packs: ' + (errA || '') + ' / ' + (errB || '');
      return;
    }
    STATE.i = STATE.dates.length - 1;
    loadDay();
  }
  tryUrls(packUrls('cam-aug.json'), 0, function(p){ ingest(p); done(); }, function(e){ errA = e; done(); });
  tryUrls(packUrls('cam-sep.json'), 0, function(p){ ingest(p); done(); }, function(e){ errB = e; done(); });
}
boot();
