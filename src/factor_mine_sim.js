// Portable cash-book + look-list for the factor-mine dashboard.
// Same leftover / fee / min-hold / hard-red rules as src/factor_mine_book.py.
(function (global) {
  const GOOD_S = 5.0, MORE_NAMES = 4, SIZEUP = 1.35;
  const CUT_LOS = 0.03, TRAIL_OFF = 0.05, BORROW_ANNUAL = 0.01;

  function finite(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  function tone(boxes, key) {
    return String((boxes || {})[key] || "missing").toLowerCase();
  }
  function camOk(got, want) {
    if (want == null || want === "") return true;
    const w = String(want).toLowerCase();
    const g = String(got || "missing").toLowerCase();
    if (w === "present") return g !== "missing";
    if (w === "missing") return g === "missing";
    return g === w;
  }
  function orderFees(shares, price, side, f) {
    if (!f || shares <= 0 || price <= 0) return 0;
    const amount = shares * price;
    const comm = Math.min(Math.max(f.commission_per_share * shares, f.commission_min_per_order),
      f.commission_max_pct_of_amount * amount);
    const plat = Math.min(Math.max(f.platform_per_share * shares, f.platform_min_per_order),
      f.platform_max_pct_of_amount * amount);
    let total = comm + plat + f.settlement_per_share * shares;
    if (side === "sell") {
      total += Math.max(f.regulatory_pct_of_amount_sell_only * amount, f.regulatory_min_per_order);
      total += Math.min(Math.max(f.taf_per_share_sell_only * shares, f.taf_min_per_order), f.taf_max_per_order);
    }
    return Math.round(total * 10000) / 10000;
  }
  function px(pack, ticker, date, which) {
    const bar = ((pack.tape || {})[ticker] || {})[date];
    if (!bar) return null;
    const v = which === "open" ? bar[0] : bar[1];
    if (v != null && Number.isFinite(Number(v))) return Number(v);
    const o = finite(bar[0]), c = finite(bar[1]);
    return o != null ? o : c;
  }
  function lotPx(pack, lot, date, which) {
    const v = px(pack, lot.ticker, date, which);
    if (v != null) return v;
    if (which === "close") {
      const o = px(pack, lot.ticker, date, "open");
      if (o != null) return o;
    }
    return Number(lot.close_px || lot.last_px || lot.entry_px);
  }
  function signedDelta(shares, neu, old, side) {
    const raw = Number(shares) * (Number(neu) - Number(old));
    return side === "long" ? raw : -raw;
  }
  function byDate(pack) {
    if (pack._by) return pack._by;
    const m = {};
    for (const r of pack.rows || []) {
      (m[r.date] || (m[r.date] = [])).push(r);
    }
    pack._by = m;
    return m;
  }
  function rowIndex(pack) {
    if (pack._ix) return pack._ix;
    const m = {};
    for (const r of pack.rows || []) m[r.date + "|" + r.ticker] = r;
    pack._ix = m;
    return m;
  }
  function matches(row, rec, mornings) {
    const uni = rec.universe || "union";
    const srcs = new Set(row.sources || []);
    if (uni !== "union" && !srcs.has(uni)) return false;
    const req = rec.require || {}, forb = rec.forbid || {};
    if (req.live_entry) {
      const ok = row.flatten_ok != null ? row.flatten_ok
        : ((mornings || {})[row.date] || {}).flatten_ok;
      if (!ok) return false;
    }
    const boxes = row.boxes || {};
    const cams = ["join","sector","gen","news","digest","judge","ab","peer","heat","vol","catal","buy"];
    for (const cam of cams) {
      if (req[cam] != null && !camOk(tone(boxes, cam), req[cam])) return false;
      if (forb[cam] != null && camOk(tone(boxes, cam), forb[cam])) return false;
    }
    if (req.blue && !row.blue) return false;
    if (req.zero_red && !row.zero_red) return false;
    if (forb.alarm && row.alarm) return false;
    if (req.alarm && !row.alarm) return false;
    if (req.last_green && !row.last_green) return false;
    if (req.last_red && !row.last_red) return false;
    if (req.candle_capture && !row.candle_capture) return false;
    if (req.break_10 && !row.ohlc_break_10) return false;
    if (req.earn_react && !row.erd_earn_react) return false;
    if (req.news_present && tone(boxes, "news") === "missing") return false;
    if (req.join_present && tone(boxes, "join") === "missing") return false;
    if (req.catal_present && tone(boxes, "catal") === "missing") return false;
    if (req.ret_5_min != null && !(row.ohlc_ret_5 != null && row.ohlc_ret_5 >= Number(req.ret_5_min))) return false;
    if (req.ret_5_max != null && !(row.ohlc_ret_5 != null && row.ohlc_ret_5 <= Number(req.ret_5_max))) return false;
    if (req.rvol_min != null && !(row.ohlc_rvol != null && row.ohlc_rvol >= Number(req.rvol_min))) return false;
    if (req.rvol_max != null && !(row.ohlc_rvol != null && row.ohlc_rvol <= Number(req.rvol_max))) return false;
    if (req.days_since_E_max != null && !(row.erd_days_since_E != null && Number(row.erd_days_since_E) <= Number(req.days_since_E_max))) return false;
    if (req.flag_R != null && Number(row.erd_flag_R || 0) !== Number(req.flag_R)) return false;
    return true;
  }
  function rankKey(row, rec) {
    const how = rec.rank;
    const hot = finite(row.ohlc_hot_score) || 0;
    const candle = finite(row.candle_score) || 0;
    const cond = (row.cond_good || 0) - (row.cond_bad || 0);
    if (how === "hot_score") return [-hot, row.ticker];
    if (how === "candle_score") return [-candle, row.ticker];
    if (how === "ret_5") return [-(finite(row.ohlc_ret_5) || 0), row.ticker];
    if (how === "cond") return [-(row.cond_good || 0), (row.cond_bad || 0), row.ticker];
    if (how === "w_hot_cond") return [-(0.6 * hot + 0.4 * Math.max(cond, 0)), row.ticker];
    if (how === "w_hot_candle") return [-(0.6 * hot + 0.4 * candle), row.ticker];
    const src = row.src_rank == null ? 99 : Number(row.src_rank);
    return [src, row.ticker];
  }
  function cmpKey(a, b) {
    for (let i = 0; i < a.length; i++) {
      if (a[i] < b[i]) return -1;
      if (a[i] > b[i]) return 1;
    }
    return 0;
  }
  function pickDay(rows, rec, mornings) {
    const kept = (rows || []).filter(r => matches(r, rec, mornings));
    kept.sort((a, b) => cmpKey(rankKey(a, rec), rankKey(b, rec)));
    return kept.slice(0, Number(rec.top_n || 8));
  }
  function rankScore(row, rec) {
    const how = rec.rank;
    const hot = finite(row.ohlc_hot_score) || 0;
    const candle = finite(row.candle_score) || 0;
    const cond = (row.cond_good || 0) - (row.cond_bad || 0);
    if (how === "hot_score") return Math.round(hot * 10000) / 10000;
    if (how === "candle_score") return Math.round(candle * 10000) / 10000;
    if (how === "ret_5") return Math.round((finite(row.ohlc_ret_5) || 0) * 10000) / 10000;
    if (how === "cond") return cond;
    if (how === "w_hot_cond") return Math.round((0.6 * hot + 0.4 * Math.max(cond, 0)) * 10000) / 10000;
    if (how === "w_hot_candle") return Math.round((0.6 * hot + 0.4 * candle) * 10000) / 10000;
    return (row.src_rank == null ? 99 : Number(row.src_rank)) * -1 + 100;
  }
  function shouldExit(row, exitWhen) {
    if (!exitWhen) return false;
    if (exitWhen.alarm && row && row.alarm) return true;
    if (exitWhen.last_red && row && row.last_red) return true;
    if (exitWhen.news === "bad" && tone(row && row.boxes, "news") === "bad") return true;
    return false;
  }
  function holdWindow(cal, date, hold) {
    const i = cal.indexOf(date);
    if (i < 0) return [];
    return cal.slice(i, i + hold);
  }
  function holdReturn(pack, rec, ticker, date) {
    const cal = pack.dates || [];
    const win = holdWindow(cal, date, Number(rec.hold || 1));
    if (!win.length) return null;
    const entry = px(pack, ticker, date, "open") || px(pack, ticker, date, "close");
    if (entry == null || entry === 0) return null;
    let exitDate = win[win.length - 1];
    let early = false;
    const ix = rowIndex(pack);
    if (rec.exit_when) {
      for (const later of win.slice(1)) {
        const nxt = ix[later + "|" + ticker];
        if (nxt && shouldExit(nxt, rec.exit_when)) {
          exitDate = later;
          early = true;
          break;
        }
      }
    }
    const exitBarWhich = early ? "open" : "close";
    let outPx = px(pack, ticker, exitDate, exitBarWhich) || px(pack, ticker, exitDate, "close") || px(pack, ticker, exitDate, "open");
    if (outPx == null) {
      const end = cal.indexOf(exitDate);
      for (let j = end; j >= 0; j--) {
        outPx = px(pack, ticker, cal[j], "close") || px(pack, ticker, cal[j], "open");
        if (outPx != null) break;
      }
    }
    if (outPx == null || outPx === 0) return null;
    let ret = 100 * (outPx / entry - 1);
    if ((rec.side || "long") === "short") ret = -ret;
    return Math.round(ret * 1000) / 1000;
  }
  function lookDay(pack, rec, date, mornings) {
    const rows = byDate(pack)[date] || [];
    const uni = rec.universe || "union";
    const looked = rows.filter(r => uni === "union" || (r.sources || []).includes(uni));
    const passed = looked.filter(r => matches(r, rec, mornings));
    passed.sort((a, b) => cmpKey(rankKey(a, rec), rankKey(b, rec)));
    const morn = (mornings || {})[date] || {};
    const s = morn.s != null ? Number(morn.s) : (pack.s || {})[date];
    const hard = !!(morn.hard_red || (s != null && s <= (pack.hard_red != null ? pack.hard_red : -3)));
    let topN = Number(rec.top_n || 8);
    if (s != null && s >= (pack.good_s || GOOD_S) && !hard
        && (rec.s_boost === "more_names" || rec.s_boost === "both")) {
      topN += pack.more_names || MORE_NAMES;
    }
    const out = [];
    const seen = new Set();
    passed.forEach((r, i) => {
      seen.add(r.ticker);
      const nNeg = Object.values(r.boxes || {}).filter(v => v === "bad").length + (r.alarm ? 1 : 0);
      out.push({
        ticker: r.ticker, rank: i + 1, score: rankScore(r, rec),
        pass: true, buy: !hard && (i + 1) <= topN,
        ret: holdReturn(pack, rec, r.ticker, date),
        n_neg: nNeg, src_rank: r.src_rank,
      });
    });
    looked.filter(r => !seen.has(r.ticker)).sort((a, b) => cmpKey(rankKey(a, rec), rankKey(b, rec)))
      .forEach(r => {
        const nNeg = Object.values(r.boxes || {}).filter(v => v === "bad").length + (r.alarm ? 1 : 0);
        out.push({
          ticker: r.ticker, rank: null, score: rankScore(r, rec),
          pass: false, buy: false,
          ret: holdReturn(pack, rec, r.ticker, date),
          n_neg: nNeg, src_rank: r.src_rank,
        });
      });
    return out;
  }
  function splitBudgets(n, room, mode) {
    if (n < 1 || room <= 0) return Array(n).fill(0);
    mode = mode || "leftover";
    if (mode === "half") {
      room *= 0.5;
      return Array(n).fill(room / n);
    }
    if (mode === "rank_w") {
      const w = [];
      for (let i = n; i >= 1; i--) w.push(i);
      const tot = w.reduce((a, b) => a + b, 0);
      return w.map(x => room * x / tot);
    }
    if (mode === "topheavy") {
      if (n === 1) return [room];
      const first = room * 0.40;
      const rest = (room - first) / (n - 1);
      return [first].concat(Array(n - 1).fill(rest));
    }
    return Array(n).fill(room / n);
  }
  function lotShouldSell(lot, held, minHold, early, dropped, sellMode, p, side) {
    if (early) return [true, "early"];
    if (held < minHold) return [false, "min_hold"];
    const mode = sellMode || "list";
    const entry = Number(lot.entry_px || 0);
    const peak = Number(lot.peak_px || entry);
    if (mode === "time") return [true, "time"];
    if (mode === "cut_loser" && p && entry) {
      if (side === "long" && p < entry * (1 - CUT_LOS)) return [true, "cut_loser"];
      if (side === "short" && p > entry * (1 + CUT_LOS)) return [true, "cut_loser"];
      if (dropped) return [true, "dropped"];
      return [false, "keep"];
    }
    if (mode === "trail" && p && peak) {
      if (side === "long" && p < peak * (1 - TRAIL_OFF)) return [true, "trail"];
      if (side === "short" && p > peak * (1 + TRAIL_OFF)) return [true, "trail"];
      if (dropped) return [true, "dropped"];
      return [false, "keep"];
    }
    if (dropped) return [true, "dropped"];
    return [false, "keep"];
  }
  function whySell(held, minHold, early, exitWhen, dropped, kind) {
    if (early) {
      if ((exitWhen || {}).alarm) return "exit 🚨 after " + held + " sess";
      if ((exitWhen || {}).last_red) return "exit last-red after " + held + " sess";
      if ((exitWhen || {}).news === "bad") return "exit news🔴 after " + held + " sess";
      return "condition exit after " + held + " sess";
    }
    if (kind === "time") return "time-stop after " + held + " sess (min " + minHold + ")";
    if (kind === "cut_loser") return "cut loser after " + held + " sess";
    if (kind === "trail") return "trail off peak after " + held + " sess";
    if (dropped) return "dropped from list after " + held + " sess (min " + minHold + ")";
    return "sold after " + held + " sess";
  }
  function whyBuy(rec, row, mornings) {
    const bits = [rec.note || rec.name];
    const req = rec.require || {};
    const shown = Object.keys(req).filter(k => k !== "live_entry");
    if (shown.length) bits.push("gate " + shown.map(k => k + "=" + req[k]).join(","));
    if (rec.rank) bits.push("rank " + rec.rank);
    const src = (row.sources || []).join(",");
    if (src) bits.push("list " + src);
    const plan = (mornings || {})[row.date] || {};
    if (rec.universe === "flatten" || req.live_entry) {
      bits.push(plan.flatten_ok
        ? ("live flatten " + (plan.route || "mover"))
        : ("wish-list (live " + (plan.route || "io") + " HOLD — not a ticket)"));
    }
    if (row.blue) bits.push("🔵");
    if (row.zero_red) bits.push("⚪");
    if (row.ohlc_ret_5 != null) bits.push("ret5=" + (Number(row.ohlc_ret_5) >= 0 ? "+" : "") + Number(row.ohlc_ret_5).toFixed(1));
    return bits.join("; ");
  }
  function markStock(pack, pos, date, which, side) {
    let tot = 0;
    for (const lot of Object.values(pos)) {
      const p = lotPx(pack, lot, date, which);
      const n = lot.shares * p;
      tot += side === "long" ? n : -n;
    }
    return tot;
  }
  function overnightMarks(pack, pos, date, side, ydayEquity, openCash) {
    const names = [];
    let openStock = 0;
    for (const [t, lot] of Object.entries(pos)) {
      const shares = lot.shares;
      const ydayPx = Number(lot.close_px || lot.last_px || lot.entry_px);
      let opx = px(pack, t, date, "open");
      if (opx == null) opx = ydayPx;
      const dlt = signedDelta(shares, opx, ydayPx, side);
      openStock += side === "long" ? shares * opx : -shares * opx;
      const entry = Number(lot.entry_px || ydayPx);
      names.push({
        ticker: t, shares,
        yday_px: Math.round(ydayPx * 10000) / 10000,
        open_px: Math.round(opx * 10000) / 10000,
        entry_px: Math.round(entry * 10000) / 10000,
        entry_date: lot.entry_date,
        delta: Math.round(dlt * 100) / 100,
        overnight: Math.round(dlt * 100) / 100,
        vs_entry_open: Math.round(signedDelta(shares, opx, entry, side) * 100) / 100,
      });
    }
    const openEq = openCash + openStock;
    return {
      open_stock: Math.round(openStock * 100) / 100,
      open_equity: Math.round(openEq * 100) / 100,
      yday_equity: Math.round(ydayEquity * 100) / 100,
      overnight_delta: Math.round((openEq - ydayEquity) * 100) / 100,
      overnight: names,
    };
  }
  function dayMarks(overnight, pos, pack, date, side) {
    const by = {};
    for (const n of overnight || []) {
      by[n.ticker] = {
        ticker: n.ticker,
        shares_open: n.shares, shares_close: 0, shares: n.shares,
        yday_px: n.yday_px, open_px: n.open_px, close_px: null,
        entry_px: n.entry_px, entry_date: n.entry_date,
        overnight: n.overnight, session: 0, day: n.overnight,
        vs_entry_open: n.vs_entry_open, vs_entry_close: null,
        held: "sold",
      };
    }
    for (const [t, lot] of Object.entries(pos)) {
      const shares = lot.shares;
      const opx = (by[t] && by[t].open_px != null) ? Number(by[t].open_px) : lotPx(pack, lot, date, "open");
      const cpx = lot.close_px != null ? Number(lot.close_px) : lotPx(pack, lot, date, "close");
      const sess = signedDelta(shares, cpx, opx, side);
      const entry = Number(lot.entry_px || opx);
      const vsClose = signedDelta(shares, cpx, entry, side);
      if (by[t]) {
        by[t].shares_close = shares;
        by[t].shares = shares;
        by[t].close_px = Math.round(cpx * 10000) / 10000;
        by[t].session = Math.round(sess * 100) / 100;
        by[t].day = Math.round((by[t].overnight + sess) * 100) / 100;
        by[t].vs_entry_close = Math.round(vsClose * 100) / 100;
        by[t].held = "through";
      } else {
        by[t] = {
          ticker: t, shares_open: 0, shares_close: shares, shares,
          yday_px: null, open_px: Math.round(opx * 10000) / 10000,
          close_px: Math.round(cpx * 10000) / 10000,
          entry_px: Math.round(entry * 10000) / 10000,
          entry_date: lot.entry_date,
          overnight: 0, session: Math.round(sess * 100) / 100,
          day: Math.round(sess * 100) / 100,
          vs_entry_open: Math.round(signedDelta(shares, opx, entry, side) * 100) / 100,
          vs_entry_close: Math.round(vsClose * 100) / 100,
          held: "bought",
        };
      }
    }
    return Object.values(by);
  }
  function simulateBook(pack, rec, start, mornings) {
    const fees = pack.fees || {};
    const calAll = pack.dates || [];
    const cal = calAll.filter(d => !start || d >= start);
    const rowsBy = byDate(pack);
    const ix = rowIndex(pack);
    const capital = Number(pack.capital || 10000);
    let cash = capital;
    const pos = {};
    const trades = [];
    const skips = [];
    const daily = [];
    const dateIx = Object.fromEntries(cal.map((d, i) => [d, i]));
    const minHold = Number(rec.hold);
    const side = rec.side || "long";
    const dayCap = Number(rec.day_cap || 1);
    const sizeMode = rec.size || "leftover";
    const sellMode = rec.sell || "list";
    const sBoost = rec.s_boost || "none";
    let ydayEquity = capital;
    const hardCut = pack.hard_red != null ? pack.hard_red : -3;

    for (const date of cal) {
      const morn = (mornings || {})[date] || {};
      const s = morn.s != null ? Number(morn.s) : (pack.s || {})[date];
      const hardRed = s != null && s <= hardCut;
      const goodS = s != null && s >= (pack.good_s || GOOD_S) && !hardRed;
      let recDay = rec;
      if (goodS && (sBoost === "more_names" || sBoost === "both")) {
        recDay = Object.assign({}, rec, { top_n: Number(rec.top_n) + (pack.more_names || MORE_NAMES) });
      }
      const chosen = pickDay(rowsBy[date] || [], recDay, mornings);
      const tset = new Set(chosen.map(r => r.ticker));
      const sold = [], bought = [];
      let heldNames = [];
      const openCash = cash;
      const openLots = Object.entries(pos).map(([t, p]) => ({ ticker: t, shares: p.shares, entry_date: p.entry_date, entry_px: p.entry_px }));
      const ov = overnightMarks(pack, pos, date, side, ydayEquity, openCash);
      trades.push({
        date, ticker: "", side: "OPEN", shares: 0, price: null, fees: 0, pnl: null,
        cash_after: Math.round(openCash * 100) / 100,
        equity_after: ov.open_equity, equity_delta: ov.overnight_delta,
        overnight_delta: ov.overnight_delta, stock_after: ov.open_stock,
        yday_equity: ov.yday_equity,
        open_held: openLots.map(p => p.ticker + "×" + p.shares),
        overnight: ov.overnight,
      });
      for (const t of Object.keys(pos)) {
        const lot = pos[t];
        const held = dateIx[date] - (dateIx[lot.entry_date] != null ? dateIx[lot.entry_date] : dateIx[date]);
        const row = ix[date + "|" + t] || {};
        const early = shouldExit(row, rec.exit_when);
        const dropped = !tset.has(t);
        let p = px(pack, t, date, "open");
        if (p != null) {
          if (side === "long") lot.peak_px = Math.max(lot.peak_px || lot.entry_px, p);
          else lot.peak_px = Math.min(lot.peak_px || lot.entry_px, p);
          lot.last_px = p;
        }
        const [doSell, kind] = lotShouldSell(lot, held, minHold, early, dropped, sellMode, p, side);
        if (!doSell) {
          if (dropped && held < minHold) {
            skips.push({ date, ticker: t, kind: "min_hold", reason: "dropped but min-hold " + held + "/" + minHold + " sess — no sell" });
          }
          heldNames.push(t);
          continue;
        }
        if (p == null) {
          skips.push({ date, ticker: t, kind: "no_price", reason: "no 09:30 open — carry" });
          heldNames.push(t);
          continue;
        }
        const eqBefore = cash + markStock(pack, pos, date, "open", side);
        const fee = orderFees(lot.shares, p, side === "long" ? "sell" : "buy", fees);
        let pnl;
        if (side === "long") {
          const proceeds = lot.shares * p - fee;
          cash += proceeds;
          pnl = proceeds - lot.cost;
        } else {
          const costCover = lot.shares * p + fee;
          cash -= costCover;
          pnl = lot.notional - costCover - (lot.fee_in || 0);
        }
        delete pos[t];
        const recT = {
          date, ticker: t, side: side === "long" ? "SELL" : "COVER",
          shares: lot.shares, price: Math.round(p * 10000) / 10000, fees: fee,
          cash_after: Math.round(cash * 100) / 100, pnl: Math.round(pnl * 100) / 100,
          reason: whySell(held, minHold, early, rec.exit_when, dropped, kind),
        };
        const stock = markStock(pack, pos, date, "open", side);
        recT.equity_after = Math.round((cash + stock) * 100) / 100;
        recT.equity_before = Math.round(eqBefore * 100) / 100;
        recT.sell_eq_chg = Math.round((recT.equity_after - eqBefore) * 100) / 100;
        recT.vs_yday = Math.round((recT.equity_after - ydayEquity) * 100) / 100;
        recT.stock_after = Math.round(stock * 100) / 100;
        trades.push(recT);
        sold.push(recT);
      }
      let neu = chosen.filter(r => !pos[r.ticker]);
      if (hardRed) {
        for (const r of neu) {
          skips.push({ date, ticker: r.ticker, kind: "hard_red", reason: "hard-red S=" + (s >= 0 ? "+" : "") + Number(s).toFixed(2) + " sit; no new buys" });
        }
        neu = [];
      }
      if (neu.length && (cash > 0 || side === "short")) {
        const eqOpen = cash + markStock(pack, pos, date, "open", side);
        let room = side === "short" ? Math.max(0, eqOpen * Math.min(dayCap, 0.5)) : Math.max(0, cash * dayCap);
        if (goodS && (sBoost === "sizeup" || sBoost === "both")) {
          room = Math.min(room * (pack.sizeup || SIZEUP), side === "long" ? cash : room * (pack.sizeup || SIZEUP));
          if (side === "long") room = Math.min(room, cash);
        }
        const budgets = splitBudgets(neu.length, room, sizeMode);
        neu.forEach((row, i) => {
          const t = row.ticker;
          const p = px(pack, t, date, "open");
          const reason = whyBuy(rec, row, mornings) + "; leftover $" + budgets[i].toFixed(2);
          if (p == null) {
            skips.push({ date, ticker: t, kind: "no_price", reason: "no 09:30 open" });
            return;
          }
          let shares = Math.floor(budgets[i] / p);
          if (shares < 1) {
            skips.push({ date, ticker: t, kind: "cash", reason: "leftover split " + budgets[i].toFixed(2) + " < 1 share @ " + p.toFixed(2) });
            return;
          }
          let fee = orderFees(shares, p, side === "long" ? "buy" : "sell", fees);
          let lot;
          if (side === "long") {
            let cost = shares * p + fee;
            if (cost > cash + 1e-6) {
              shares = p ? Math.floor((cash - fee) / p) : 0;
              if (shares < 1) {
                skips.push({ date, ticker: t, kind: "cash", reason: "cash " + cash.toFixed(2) + " < 1 share @ " + p.toFixed(2) });
                return;
              }
              fee = orderFees(shares, p, "buy", fees);
              cost = shares * p + fee;
            }
            cash -= cost;
            lot = { ticker: t, shares, entry_px: p, entry_date: date, cost, fee_in: fee, notional: shares * p, last_px: p, peak_px: p, reason };
          } else {
            const notional = shares * p;
            const eqNow = cash + markStock(pack, pos, date, "open", side);
            if (eqNow < 2 * notional) {
              skips.push({ date, ticker: t, kind: "cash", reason: "short cover " + (2 * notional).toFixed(0) + " > equity " + eqNow.toFixed(0) });
              return;
            }
            const borrow = notional * (pack.borrow_annual || BORROW_ANNUAL) / 365;
            fee = orderFees(shares, p, "sell", fees) + borrow;
            cash += notional - fee;
            lot = { ticker: t, shares, entry_px: p, entry_date: date, cost: fee, fee_in: fee, notional, last_px: p, peak_px: p, reason };
          }
          pos[t] = lot;
          const recT = {
            date, ticker: t, side: side === "long" ? "BUY" : "SHORT",
            shares, price: Math.round(p * 10000) / 10000, fees: fee,
            cash_after: Math.round(cash * 100) / 100, pnl: null, reason,
          };
          const stock = markStock(pack, pos, date, "open", side);
          recT.equity_after = Math.round((cash + stock) * 100) / 100;
          recT.stock_after = Math.round(stock * 100) / 100;
          recT.equity_delta = Math.round((recT.equity_after - capital) * 100) / 100;
          trades.push(recT);
          bought.push(recT);
          heldNames.push(t);
        });
      }
      for (const t of Object.keys(pos)) {
        if (!heldNames.includes(t)) heldNames.push(t);
      }
      for (const lot of Object.values(pos)) {
        lot.close_px = lotPx(pack, lot, date, "close");
      }
      const stock = markStock(pack, pos, date, "close", side);
      const equity = cash + stock;
      const marks = dayMarks(ov.overnight, pos, pack, date, side);
      const sessSum = Math.round(marks.reduce((a, m) => a + Number(m.session || 0), 0) * 100) / 100;
      const closeHeld = Object.entries(pos).map(([t, p]) => t + "×" + p.shares);
      trades.push({
        date, ticker: "", side: "CLOSE", shares: 0, price: null, fees: 0, pnl: null,
        cash_after: Math.round(cash * 100) / 100,
        equity_after: Math.round(equity * 100) / 100,
        equity_delta: Math.round((equity - ov.open_equity) * 100) / 100,
        session_delta: sessSum, stock_after: Math.round(stock * 100) / 100,
        open_equity: ov.open_equity,
        marks, intraday: marks.filter(m => m.shares_close),
        close_held: closeHeld,
      });
      daily.push({
        date, s: s == null ? null : Math.round(Number(s) * 100) / 100,
        hard_red: hardRed,
        flatten_ok: !!morn.flatten_ok,
        n: chosen.length,
        open_cash: Math.round(openCash * 100) / 100,
        open_held: openLots.map(p => p.ticker + "×" + p.shares),
        open_equity: ov.open_equity,
        yday_equity: ov.yday_equity,
        overnight_delta: ov.overnight_delta,
        session_delta: sessSum,
        cash: Math.round(cash * 100) / 100,
        stock: Math.round(stock * 100) / 100,
        equity: Math.round(equity * 100) / 100,
        bought: bought.map(b => b.ticker),
        sold: sold.map(x => x.ticker),
        held: Object.keys(pos),
        lots: Object.entries(pos).map(([t, p]) => ({ ticker: t, shares: p.shares, entry_date: p.entry_date, entry_px: p.entry_px })),
        marks, made_money: false,
      });
      ydayEquity = Math.round(equity * 100) / 100;
    }
    daily.forEach((d, i) => {
      const prev = i === 0 ? capital : daily[i - 1].equity;
      d.mean = prev <= 0 ? null : Math.round(10000 * (d.equity / prev - 1)) / 100;
      d.made_money = d.mean != null && d.mean > 0;
    });
    const eq = [capital].concat(daily.map(d => d.equity));
    const totalRet = eq.length ? Math.round(1000 * (eq[eq.length - 1] / capital - 1) * 10) / 10 : 0;
    // Python uses round(..., 3) on percent — match 3 decimals via 1000 then /10? 
    // 100.0 * (eq[-1]/cap - 1) rounded to 3. Use:
    const total_ret_pct = eq.length ? Math.round(1000 * (100 * (eq[eq.length - 1] / capital - 1))) / 1000 : 0;
    return {
      name: rec.name,
      cash: Math.round(cash * 100) / 100,
      total_ret_pct,
      final_equity: eq[eq.length - 1],
      equity: eq.map(x => Math.round(x * 100) / 100),
      daily, trades, skips,
      n_trades: trades.filter(t => t.side !== "OPEN" && t.side !== "CLOSE").length,
      n_skips: skips.length,
    };
  }

  global.FMSim = {
    matches, pickDay, rankScore, lookDay, holdReturn, simulateBook, orderFees,
  };
})(typeof window !== "undefined" ? window : globalThis);
