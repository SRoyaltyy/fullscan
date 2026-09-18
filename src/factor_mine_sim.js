// Portable cash-book + look-list for the factor-mine dashboard.
// Same leftover / fee / min-hold / hard-red rules as src/factor_mine_book.py.
(function (global) {
  const GOOD_S = 5.0, MORE_NAMES = 4, SIZEUP = 1.35;
  const CUT_LOS = 0.03, TRAIL_OFF = 0.05, BORROW_ANNUAL = 0.01;

  function finite(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  function camGood(row) {
    if (row.cond_good != null) return Number(row.cond_good) || 0;
    return Object.entries(row.boxes || {}).filter(([k, v]) => k !== "yday" && v === "good").length;
  }
  function camBad(row) {
    if (row.cond_bad != null) return Number(row.cond_bad) || 0;
    return Object.entries(row.boxes || {}).filter(([k, v]) => k !== "yday" && v === "bad").length;
  }
  function ydayRet(pack, row, date) {
    const stored = finite(row && row.ohlc_ret_1);
    if (stored != null) return Math.round(stored * 100) / 100;
    const cal = (pack && pack.dates) || [];
    const d = date || (row && row.date);
    const i = cal.indexOf(d);
    if (i < 2 || !row) return null;
    const c1 = px(pack, row.ticker, cal[i - 1], "close");
    const c0 = px(pack, row.ticker, cal[i - 2], "close");
    if (c1 == null || c0 == null || c0 === 0) return null;
    return Math.round(10000 * (c1 / c0 - 1)) / 100;
  }
  function ydayUp(row, pack, date) {
    const v = pack ? ydayRet(pack, row, date) : finite(row && row.ohlc_ret_1);
    if (v != null) return v > 0;
    return !!(row && row.last_green);
  }
  function majorCatalyst(row) {
    const boxes = (row && row.boxes) || {};
    if (String(boxes.catal || "").toLowerCase() === "good") return true;
    if (String((row && row.catal) || "").toLowerCase() === "good") return true;
    const ep = String((row && row.e_pol) || "").toLowerCase();
    if (ep === "good") return true;
    const earn = !!(row && (row.erd_earn_react || row.earn_react));
    return earn && ep !== "bad";
  }
  function whiteHorizonOverlay(rec) {
    rec = rec || {};
    const name = String(rec.name || "looker");
    return {
      name: name.endsWith("_white_any") ? name : name + "_white_any",
      universe: rec.universe || "union",
      hold: Number(rec.hold || 1),
      side: rec.side || "long",
      top_n: Number(rec.top_n || 8),
      require: {cam_bad_max: 0, yday_or_catalyst: true},
      forbid: {alarm: true},
      rank: "list",
      size: rec.size || "leftover",
      sell: rec.sell || "list",
      s_boost: rec.s_boost || "none",
      take_pct: rec.take_pct,
      stop_pct: rec.stop_pct,
      looker: name,
    };
  }
  function whiteYdayOverlay(rec) {
    rec = rec || {};
    const name = String(rec.name || "looker");
    return {
      name: name.endsWith("_white_yday") ? name : name + "_white_yday",
      universe: rec.universe || "union",
      hold: Number(rec.hold || 1),
      side: rec.side || "long",
      top_n: Number(rec.top_n || 8),
      require: {zero_red: true, yday_up: true},
      forbid: {alarm: true},
      rank: "cond",
      size: rec.size || "leftover",
      sell: rec.sell || "list",
      s_boost: rec.s_boost || "none",
      take_pct: rec.take_pct,
      stop_pct: rec.stop_pct,
      looker: name,
    };
  }
  function recipeSide(rec, row) {
    const s = String((row && row.side) || (rec && rec.side) || "long").toLowerCase();
    return s === "short" ? "short" : "long";
  }
  function takeBucket(row, hard) {
    if (hard) return "sit";
    if (row && row.buy) return "buy";
    if (row && row.pass) return "look";
    return "no";
  }
  function polarityPredict(row, rec, hard) {
    const side = recipeSide(rec, row);
    const bucket = takeBucket(row, hard);
    // BUY / LOOK / SIT keep the sleeve's intended direction.
    // NO is a skip on that sleeve, so the call is the other way.
    if (bucket === "no") return side === "short" ? "UP" : "DOWN";
    return side === "short" ? "DOWN" : "UP";
  }
  function polarityHit(predict, pxRet) {
    if (pxRet == null || !Number.isFinite(Number(pxRet)) || Number(pxRet) === 0) return null;
    if (predict === "UP") return Number(pxRet) > 0;
    if (predict === "DOWN") return Number(pxRet) < 0;
    return null;
  }
  function packPolarity(flags) {
    const v = (flags || []).filter(x => x != null);
    if (!v.length) return {n: 0, hit: 0, miss: 0, win: null};
    const hit = v.filter(Boolean).length;
    return {
      n: v.length,
      hit,
      miss: v.length - hit,
      win: Math.round(10000 * hit / v.length) / 10000,
    };
  }
  function lookStamp(pack, rec, r, date, extra) {
    const nGood = camGood(r);
    const nBad = camBad(r);
    const nNeg = nBad + (r.alarm ? 1 : 0);
    const yr = ydayRet(pack, r, date);
    const extra2 = extra || {};
    const side = extra2.side || rec.side || "long";
    const fill = holdFill(pack, rec, r.ticker, date);
    const signed = fill.ret;
    const pxRet = signed == null ? null
      : ((String(side).toLowerCase() === "short") ? -signed : signed);
    const stamp = Object.assign({
      ticker: r.ticker,
      score: rankScore(r, rec),
      ret: signed,
      px_ret: pxRet,
      buy_px: fill.buy_px,
      sell_px: fill.sell_px,
      fill_reason: fill.reason,
      side,
      n_neg: nNeg,
      n_pos: nGood,
      cond_good: nGood,
      cond_bad: nBad,
      yday_ret: yr,
      yday_up: yr != null ? yr > 0 : !!r.last_green,
      src_rank: r.src_rank,
      e_pol: r.e_pol || "",
      e_label: r.e_label || "",
      earn_react: !!r.erd_earn_react,
      rsi: r.rsi,
      fv_rsi: r.fv_rsi,
      macd_hist: r.macd_hist,
      macd_cross_up: !!r.macd_cross_up,
      rsi_os: !!r.rsi_os,
      rsi_ob: !!r.rsi_ob,
      macd_up: !!r.macd_up,
      flow_in: !!r.flow_in,
    }, extra2);
    stamp.predict = polarityPredict(stamp, rec, false);
    stamp.pol_hit = polarityHit(stamp.predict, stamp.px_ret);
    return stamp;
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
  function sessionHasClosed(date, now) {
    now = now || new Date();
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York", year: "numeric", month: "2-digit",
      day: "2-digit", hour: "2-digit", minute: "2-digit", hourCycle: "h23"
    }).formatToParts(now);
    const get = t => parts.find(p => p.type === t).value;
    const today = get("year") + "-" + get("month") + "-" + get("day");
    if (!date) return false;
    if (date < today) return true;
    if (date > today) return false;
    const hm = Number(get("hour")) * 60 + Number(get("minute"));
    return hm >= 16 * 60 + 2;
  }
  function dateHasClose(pack, date) {
    const tape = pack.tape || {};
    for (const t of Object.keys(tape)) {
      const bar = tape[t] && tape[t][date];
      if (bar && bar[1] != null && Number.isFinite(Number(bar[1])) && Number(bar[1]) > 0)
        return true;
    }
    return false;
  }
  function lastClosedDate(pack) {
    const dates = pack.dates || [];
    for (let i = dates.length - 1; i >= 0; i--) {
      const d = dates[i];
      if (dateHasClose(pack, d) || sessionHasClosed(d)) return d;
    }
    return dates.length ? dates[dates.length - 1] : null;
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
    if (req.flag_E_min != null && !(row.erd_flag_E != null && Number(row.erd_flag_E) >= Number(req.flag_E_min))) return false;
    if (req.days_since_R_max != null && !(row.erd_days_since_R != null && Number(row.erd_days_since_R) <= Number(req.days_since_R_max))) return false;
    if (req.flag_R != null && Number(row.erd_flag_R || 0) !== Number(req.flag_R)) return false;
    const nNeg = Object.values(row.boxes || {}).filter(v => v === "bad").length + (row.alarm ? 1 : 0);
    if (req.n_neg_max != null && nNeg > Number(req.n_neg_max)) return false;
    if (req.n_neg_min != null && nNeg < Number(req.n_neg_min)) return false;
    if (req.n_pos_min != null && camGood(row) < Number(req.n_pos_min)) return false;
    if (req.news_box != null && !camOk(String(row.news_box || "missing").toLowerCase(), req.news_box)) return false;
    if (req.headline != null && !camOk(String(row.news_prior || "missing").toLowerCase(), req.headline)) return false;
    if (req.news_and_headline && !(String(row.news_box || "").toLowerCase() === "good"
        && String(row.news_prior || "").toLowerCase() === "good")) return false;
    if (req.news_or_headline && !(String(row.news_box || "").toLowerCase() === "good"
        || String(row.news_prior || "").toLowerCase() === "good")) return false;
    if (req.news_or_red && !(String(row.news_box || "").toLowerCase() === "bad"
        || String(row.news_prior || "").toLowerCase() === "bad")) return false;
    if (req.cam_net_min != null && (camGood(row) - camBad(row)) < Number(req.cam_net_min)) return false;
    if (req.yday_up && !ydayUp(row)) return false;
    if (req.cam_bad_max != null && camBad(row) > Number(req.cam_bad_max)) return false;
    if (req.major_catalyst && !majorCatalyst(row)) return false;
    if (req.yday_or_catalyst && !(ydayUp(row) || majorCatalyst(row))) return false;
    if (req.yday_and_catalyst && !(ydayUp(row) && majorCatalyst(row))) return false;
    if (req.burst) {
      const ret = finite(row.ohlc_ret_5), rvol = finite(row.ohlc_rvol);
      const burst = ret != null && ret >= 12 && !!row.last_green
        && (!!row.ohlc_break_10 || (rvol != null && rvol >= 2));
      if (!burst) return false;
    }
    if (req.rsi_os && !row.rsi_os) return false;
    if (req.rsi_ob && !row.rsi_ob) return false;
    if (req.macd_up && !row.macd_up) return false;
    if (req.macd_down && !row.macd_down) return false;
    if (req.macd_cross_up && !row.macd_cross_up) return false;
    if (req.flow_in && !row.flow_in) return false;
    if (req.rsi_min != null && !(row.rsi != null && row.rsi >= Number(req.rsi_min))) return false;
    if (req.rsi_max != null && !(row.rsi != null && row.rsi <= Number(req.rsi_max))) return false;
    return true;
  }
  function kidGate(key, val) {
    if (key === "live_entry") return "live flatten gate says GO";
    if (key === "blue") return "the name is painted 🔵";
    if (key === "zero_red") return "no morning camera is red";
    if (key === "alarm") return "the 🚨 alarm is on";
    if (key === "last_green") return "the last finished bar was green";
    if (key === "last_red") return "the last finished bar was red";
    if (key === "candle_capture") return "the prior-candle capture flag is on";
    if (key === "break_10") return "the name broke its prior 10-session range";
    if (key === "earn_react") return "the name is in an earnings-reaction window";
    if (key === "news_present") return "the news camera printed something";
    if (key === "join_present") return "the join camera printed something";
    if (key === "catal_present") return "the catalyst camera printed something";
    if (key === "yday_up") return "yesterday's session was up";
    if (key === "cam_bad_max") return "at most " + val + " red cameras (−R, no 🚨)";
    if (key === "n_pos_min") return "at least " + val + " green cameras (+G)";
    if (key === "news_box") return "the morning news packet is " + (val === "good" ? "green" : val);
    if (key === "headline") return "the prior-export headline is " + (val === "good" ? "green" : val);
    if (key === "news_and_headline") return "packet and headline are both green";
    if (key === "news_or_headline") return "packet or headline is green";
    if (key === "news_or_red") return "packet or headline is red";
    if (key === "cam_net_min") return "camera net (+G −R) is at least " + val;
    if (key === "yday_or_catalyst") return "yesterday up or a major good catalyst";
    if (key === "yday_and_catalyst") return "yesterday up AND a major good catalyst";
    if (key === "major_catalyst") return "a major good catalyst";
    if (key === "ret_5_min") return "prior 5-session return is at least " + val + "%";
    if (key === "ret_5_max") return "prior 5-session return is at most " + val + "%";
    if (key === "rvol_min") return "prior relative volume is at least " + val;
    if (key === "rvol_max") return "prior relative volume is at most " + val;
    if (key === "rsi_os") return "prior RSI is oversold (≤30)";
    if (key === "rsi_ob") return "prior RSI is overbought (≥70)";
    if (key === "macd_up") return "prior MACD histogram is above zero";
    if (key === "macd_down") return "prior MACD histogram is below zero";
    if (key === "macd_cross_up") return "MACD histogram just crossed up through zero";
    if (key === "flow_in") return "money came in (rel vol ≥ 1.5) but price barely moved";
    if (key === "rsi_min") return "prior RSI is at least " + val;
    if (key === "rsi_max") return "prior RSI is at most " + val;
    if (key === "days_since_E_max") return "earnings (E) printed within the last " + val + " session(s)";
    if (key === "flag_E_min") return "the earnings flag is on";
    if (key === "days_since_R_max") return "an analyst revision (R) printed within the last " + val + " session(s)";
    if (key === "flag_R") {
      if (Number(val) === 1) return "the latest revision flag is an upgrade";
      if (Number(val) === -1) return "the latest revision flag is a downgrade";
      return "the revision flag equals " + val;
    }
    const word = {good:"green", bad:"red", neutral:"yellow", missing:"blank", true:"green"}[String(val)] || String(val);
    return key + " is " + word;
  }
  function matchWhy(row, rec, mornings) {
    const failed = [], passed = [];
    const need = (ok, msg) => { (ok ? passed : failed).push(msg); };
    const uni = rec.universe || "union";
    const srcs = new Set(row.sources || []);
    if (uni !== "union") need(srcs.has(uni), "on the " + uni + " 09:30 list");
    const req = rec.require || {}, forb = rec.forbid || {};
    if (req.live_entry) {
      const ok = row.flatten_ok != null ? row.flatten_ok
        : ((mornings || {})[row.date] || {}).flatten_ok;
      need(!!ok, "live flatten gate says GO");
    }
    const boxes = row.boxes || {};
    const cams = ["join","sector","gen","news","digest","judge","ab","peer","heat","vol","catal","buy"];
    for (const cam of cams) {
      if (req[cam] != null) need(camOk(tone(boxes, cam), req[cam]), kidGate(cam, req[cam]));
      if (forb[cam] != null) need(!camOk(tone(boxes, cam), forb[cam]), "not " + kidGate(cam, forb[cam]));
    }
    if (req.blue) need(!!row.blue, kidGate("blue", true));
    if (req.zero_red) need(!!row.zero_red, kidGate("zero_red", true));
    if (forb.alarm) need(!row.alarm, "no 🚨 overnight alarm");
    if (req.alarm) need(!!row.alarm, "🚨 alarm is on");
    if (req.last_green) need(!!row.last_green, kidGate("last_green", true));
    if (req.last_red) need(!!row.last_red, kidGate("last_red", true));
    if (req.candle_capture) need(!!row.candle_capture, kidGate("candle_capture", true));
    if (req.break_10) need(!!row.ohlc_break_10, kidGate("break_10", true));
    if (req.earn_react) need(!!row.erd_earn_react, kidGate("earn_react", true));
    if (req.news_present) need(tone(boxes, "news") !== "missing", kidGate("news_present", true));
    if (req.join_present) need(tone(boxes, "join") !== "missing", kidGate("join_present", true));
    if (req.catal_present) need(tone(boxes, "catal") !== "missing", kidGate("catal_present", true));
    if (req.ret_5_min != null) need(row.ohlc_ret_5 != null && row.ohlc_ret_5 >= Number(req.ret_5_min), kidGate("ret_5_min", req.ret_5_min));
    if (req.ret_5_max != null) need(row.ohlc_ret_5 != null && row.ohlc_ret_5 <= Number(req.ret_5_max), kidGate("ret_5_max", req.ret_5_max));
    if (req.rvol_min != null) need(row.ohlc_rvol != null && row.ohlc_rvol >= Number(req.rvol_min), kidGate("rvol_min", req.rvol_min));
    if (req.rvol_max != null) need(row.ohlc_rvol != null && row.ohlc_rvol <= Number(req.rvol_max), kidGate("rvol_max", req.rvol_max));
    if (req.days_since_E_max != null) need(row.erd_days_since_E != null && Number(row.erd_days_since_E) <= Number(req.days_since_E_max), kidGate("days_since_E_max", req.days_since_E_max));
    if (req.flag_E_min != null) need(row.erd_flag_E != null && Number(row.erd_flag_E) >= Number(req.flag_E_min), kidGate("flag_E_min", req.flag_E_min));
    if (req.days_since_R_max != null) need(row.erd_days_since_R != null && Number(row.erd_days_since_R) <= Number(req.days_since_R_max), kidGate("days_since_R_max", req.days_since_R_max));
    if (req.flag_R != null) need(Number(row.erd_flag_R || 0) === Number(req.flag_R), kidGate("flag_R", req.flag_R));
    if (req.yday_up) need(ydayUp(row), kidGate("yday_up", true));
    if (req.cam_bad_max != null) need(camBad(row) <= Number(req.cam_bad_max), kidGate("cam_bad_max", req.cam_bad_max));
    if (req.n_pos_min != null) need(camGood(row) >= Number(req.n_pos_min), kidGate("n_pos_min", req.n_pos_min));
    if (req.news_box) need(camOk(String(row.news_box || "missing").toLowerCase(), req.news_box), kidGate("news_box", req.news_box));
    if (req.headline) need(camOk(String(row.news_prior || "missing").toLowerCase(), req.headline), kidGate("headline", req.headline));
    if (req.news_and_headline) need(String(row.news_box || "").toLowerCase() === "good" && String(row.news_prior || "").toLowerCase() === "good", kidGate("news_and_headline", true));
    if (req.news_or_headline) need(String(row.news_box || "").toLowerCase() === "good" || String(row.news_prior || "").toLowerCase() === "good", kidGate("news_or_headline", true));
    if (req.news_or_red) need(String(row.news_box || "").toLowerCase() === "bad" || String(row.news_prior || "").toLowerCase() === "bad", kidGate("news_or_red", true));
    if (req.cam_net_min != null) need((camGood(row) - camBad(row)) >= Number(req.cam_net_min), kidGate("cam_net_min", req.cam_net_min));
    if (req.major_catalyst) need(majorCatalyst(row), kidGate("major_catalyst", true));
    if (req.yday_or_catalyst) need(ydayUp(row) || majorCatalyst(row), kidGate("yday_or_catalyst", true));
    if (req.yday_and_catalyst) need(ydayUp(row) && majorCatalyst(row), kidGate("yday_and_catalyst", true));
    if (req.rsi_os) need(!!row.rsi_os, kidGate("rsi_os", true));
    if (req.rsi_ob) need(!!row.rsi_ob, kidGate("rsi_ob", true));
    if (req.macd_up) need(!!row.macd_up, kidGate("macd_up", true));
    if (req.macd_down) need(!!row.macd_down, kidGate("macd_down", true));
    if (req.macd_cross_up) need(!!row.macd_cross_up, kidGate("macd_cross_up", true));
    if (req.flow_in) need(!!row.flow_in, kidGate("flow_in", true));
    if (req.rsi_min != null) need(row.rsi != null && row.rsi >= Number(req.rsi_min), kidGate("rsi_min", req.rsi_min));
    if (req.rsi_max != null) need(row.rsi != null && row.rsi <= Number(req.rsi_max), kidGate("rsi_max", req.rsi_max));
    return {ok: !failed.length, failed, passed};
  }
  function decisionWhy(pack, rec, date, ticker, mornings) {
    const looks = lookDay(pack, rec, date, mornings);
    const hit = looks.find(x => x.ticker === ticker);
    const morn = (mornings || {})[date] || {};
    const s = morn.s != null ? morn.s : (pack.s || {})[date];
    const hard = !!(morn.hard_red || (s != null && s <= (pack.hard_red != null ? pack.hard_red : -3)));
    const row = rowIndex(pack)[date + "|" + ticker];
    const mw = row ? matchWhy(row, rec, mornings)
      : {ok: false, failed: ["not on any 09:30 shopping list"], passed: []};
    const topN = Number(rec.top_n || 8);
    const lines = [];
    if (hard) {
      lines.push("Morning weather S is " + (s == null ? "—" : Number(s).toFixed(2)) + " (hard-red ≤ −3).");
      lines.push("The sleeve sits — no new lots today, even if this name would pass the buy gates.");
      if (mw.passed.length) lines.push("Gates that already pass: " + mw.passed.join("; ") + ".");
      if (mw.failed.length) lines.push("It would still fail these gates if S were above −3: " + mw.failed.join("; ") + ".");
      return {take: "sit", lines};
    }
    if (hit && hit.buy) {
      lines.push("Would buy: on this recipe's shopping list and inside the cash cut.");
      if (mw.passed.length) lines.push("Gates that fired: " + mw.passed.join("; ") + ".");
      if (hit.rank != null) lines.push("Ranked #" + hit.rank + " of top " + topN + " by " + (rec.rank || "list order") + ".");
      return {take: "buy", lines};
    }
    if (hit && hit.pass) {
      lines.push("Would not buy: passed the gates but ranked #" + hit.rank + " — only the top " + topN + " get leftover cash.");
      return {take: "no", lines};
    }
    lines.push("Would not buy.");
    if (mw.failed.length) lines.push("Failed: " + mw.failed.join("; ") + ".");
    else lines.push("Not on this recipe's 09:30 universe.");
    return {take: "no", lines};
  }
  function packRets(xs) {
    const v = (xs || []).filter(x => x != null && Number.isFinite(Number(x))).map(Number);
    if (!v.length) return {n: 0, win: null, mean: null};
    return {
      n: v.length,
      win: Math.round(10000 * v.filter(x => x > 0).length / v.length) / 10000,
      mean: Math.round(1000 * v.reduce((a, b) => a + b, 0) / v.length) / 1000,
    };
  }
  function hitTally(pack, rec, mornings) {
    const dates = pack.dates || [];
    const buy = [], no = [], sit = [], look = [], le2 = [], ge3 = [];
    const polBuy = [], polLook = [], polNo = [], polSit = [];
    for (const date of dates) {
      const looks = lookDay(pack, rec, date, mornings);
      const morn = (mornings || {})[date] || {};
      const s = morn.s != null ? morn.s : (pack.s || {})[date];
      const hard = !!(morn.hard_red || (s != null && s <= (pack.hard_red != null ? pack.hard_red : -3)));
      for (const x of looks) {
        const pred = polarityPredict(x, rec, hard);
        const hit = polarityHit(pred, x.px_ret);
        if (hard) polSit.push(hit);
        else if (x.buy) polBuy.push(hit);
        else if (x.pass) polLook.push(hit);
        else polNo.push(hit);
        if (x.ret == null) continue;
        if (hard) sit.push(x.ret);
        else if (x.buy) buy.push(x.ret);
        else if (x.pass) look.push(x.ret);
        else no.push(x.ret);
        ((x.n_neg || 0) >= 3 ? ge3 : le2).push(x.ret);
      }
    }
    return {
      buy: packRets(buy), look: packRets(look), no: packRets(no), sit: packRets(sit),
      n_neg_le2: packRets(le2), n_neg_ge3: packRets(ge3),
      pol_buy: packPolarity(polBuy), pol_look: packPolarity(polLook),
      pol_no: packPolarity(polNo), pol_sit: packPolarity(polSit),
    };
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
    if (how === "rsi") return [row.rsi == null ? 999 : Number(row.rsi), row.ticker];
    if (how === "macd_hist") return [-(finite(row.macd_hist) || 0), row.ticker];
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
    if (how === "rsi") return row.rsi == null ? null : Math.round(Number(row.rsi) * 100) / 100;
    if (how === "macd_hist") return Math.round((finite(row.macd_hist) || 0) * 10000) / 10000;
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
  function holdFill(pack, rec, ticker, date) {
    const empty = { buy_px: null, sell_px: null, ret: null, reason: null };
    const cal = pack.dates || [];
    const win = holdWindow(cal, date, Number(rec.hold || 1));
    if (!win.length) return empty;
    const entry = px(pack, ticker, date, "open") || px(pack, ticker, date, "close");
    if (entry == null || entry === 0) return empty;
    const side = String(rec.side || "long").toLowerCase() === "short" ? "short" : "long";
    const ix = rowIndex(pack);
    const dateIx = Object.fromEntries(cal.map((d, i) => [d, i]));
    const minHold = Number(rec.hold || 1);
    const lot = { entry_px: entry, peak_px: entry };
    let exitDate = win[win.length - 1];
    let early = false;
    let reason = "horizon";
    for (const later of win.slice(1)) {
      const nxt = ix[later + "|" + ticker];
      const cond = !!(rec.exit_when && shouldExit(nxt, rec.exit_when));
      const p = px(pack, ticker, later, "open");
      if (p == null) continue;
      if (side === "long") lot.peak_px = Math.max(lot.peak_px || entry, p);
      else lot.peak_px = Math.min(lot.peak_px || entry, p);
      const held = (dateIx[later] != null ? dateIx[later] : dateIx[date]) - dateIx[date];
      const [doSell, kind] = lotShouldSell(
        lot, held, minHold, cond, false, rec.sell || "list", p, side,
        rec.take_pct, rec.stop_pct);
      if (doSell && (cond || kind === "take" || kind === "stop" || kind === "early")) {
        exitDate = later;
        early = true;
        reason = cond || kind === "early" ? "early" : kind;
        break;
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
    if (outPx == null || outPx === 0) {
      return { buy_px: roundPx(entry), sell_px: null, ret: null, reason: null };
    }
    let ret = 100 * (outPx / entry - 1);
    if (side === "short") ret = -ret;
    return {
      buy_px: roundPx(entry),
      sell_px: roundPx(outPx),
      ret: Math.round(ret * 1000) / 1000,
      reason,
    };
  }
  function roundPx(p) {
    if (p == null || !Number.isFinite(Number(p))) return null;
    return Math.round(Number(p) * 100) / 100;
  }
  function holdReturn(pack, rec, ticker, date) {
    return holdFill(pack, rec, ticker, date).ret;
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
      out.push(lookStamp(pack, rec, r, date, {
        rank: i + 1, pass: true, buy: !hard && (i + 1) <= topN,
        side: rec.side || "long",
      }));
    });
    looked.filter(r => !seen.has(r.ticker)).sort((a, b) => cmpKey(rankKey(a, rec), rankKey(b, rec)))
      .forEach(r => {
        out.push(lookStamp(pack, rec, r, date, {
          rank: null, pass: false, buy: false, side: rec.side || "long",
        }));
      });
    return out;
  }
  function splitBudgets(n, room, mode, rows) {
    if (n < 1 || room <= 0) return Array(n).fill(0);
    mode = mode || "leftover";
    if (mode === "half") {
      room *= 0.5;
      return Array(n).fill(room / n);
    }
    if (mode === "conviction") {
      const top = (rows && rows[0]) || {};
      if ((camGood(top) - camBad(top)) >= 5) {
        if (n === 1) return [room];
        const first = room * 0.70;
        const rest = (room - first) / (n - 1);
        return [first].concat(Array(n - 1).fill(rest));
      }
      mode = "rank_w";
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
  function lotOpenRet(lot, p, side) {
    const entry = Number(lot && lot.entry_px || 0);
    if (p == null || !entry) return null;
    return side === "long" ? (p / entry - 1) : ((entry - p) / entry);
  }
  function lotShouldSell(lot, held, minHold, early, dropped, sellMode, p, side, takePct, stopPct) {
    if (early) return [true, "early"];
    const take = takePct != null && Number(takePct) > 0 ? Number(takePct) : null;
    const stop = stopPct != null && Number(stopPct) > 0 ? Number(stopPct) : null;
    const ret = lotOpenRet(lot, p, side);
    if (ret != null) {
      if (take != null && ret >= take) return [true, "take"];
      if (stop != null && ret <= -stop) return [true, "stop"];
    }
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
    if (kind === "take") return "take-profit after " + held + " sess";
    if (kind === "stop") return "stop-loss after " + held + " sess";
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
    // Browser replay is strict: no short entries without a dated locate.
    const risk = Object.assign({max_gross:1, max_short:0.5, short_margin:0.5, require_locate:true}, pack.risk || {});
    const calAll = pack.dates || [];
    const lastClosed = lastClosedDate(pack);
    const cal = calAll.filter(d =>
      (!start || d >= start) && (!lastClosed || d <= lastClosed));
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
      if (side === "short") {
        for (const [t, lot] of Object.entries(pos)) {
          const days = (Date.parse(date)-Date.parse(lot.borrow_through || lot.entry_date))/86400000;
          if (days > 0) {
            const fee = lot.shares * lotPx(pack,lot,date,"open") * lot.borrow_annual * days / 365;
            cash -= fee; lot.fee_in += fee; lot.cost += fee; lot.borrow_through = date;
            trades.push({date,ticker:t,side:"BORROW",shares:0,price:null,fees:fee,pnl:null,cash_after:cash});
          }
        }
      }
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
        const [doSell, kind] = lotShouldSell(lot, held, minHold, early, dropped, sellMode, p, side, rec.take_pct, rec.stop_pct);
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
          shares: lot.shares, price: p, fees: fee,
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
        const budgets = splitBudgets(neu.length, room, sizeMode, neu);
        neu.forEach((row, i) => {
          const t = row.ticker;
          const p = px(pack, t, date, "open");
          const reason = whyBuy(rec, row, mornings) + "; leftover $" + budgets[i].toFixed(2);
          if (p == null) {
            skips.push({ date, ticker: t, kind: "no_price", reason: "no 09:30 open" });
            return;
          }
          let rate = 0;
          const stockNow = markStock(pack,pos,date,"open",side);
          const eqNowRisk = cash + stockNow;
          let budget = Math.min(budgets[i], cash);
          if (side === "short") {
            const locate = (((pack.borrow || {})[date] || {})[t]);
            // Compare wall-clock ET via Intl, avoiding a fixed DST offset.
            const seen = locate && new Date(locate.observed_at);
            const parts = seen && Number.isFinite(seen.getTime()) ? Object.fromEntries(
              new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'}).formatToParts(seen).map(x=>[x.type,x.value])) : null;
            const observed = parts && `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}`;
            const located = locate && locate.available === true && observed && observed <= date+'T09:30:00' && Number.isFinite(Number(locate.annual_rate)) && Number(locate.annual_rate)>=0;
            if (risk.require_locate && !located) {
              skips.push({date,ticker:t,kind:'borrow_unavailable',reason:'No pre-open dated locate'}); return;
            }
            rate = located ? Number(locate.annual_rate) : Number(pack.borrow_annual || BORROW_ANNUAL);
            const shortValue = -stockNow;
            budget = Math.min(budgets[i], eqNowRisk*risk.max_gross-shortValue,
              eqNowRisk*risk.max_short-shortValue, (cash-(1+risk.short_margin)*shortValue)/risk.short_margin);
          } else {
            budget = Math.min(budget, eqNowRisk*risk.max_gross-stockNow);
          }
          let shares = Math.floor(Math.max(0,budget) / p);
          while(shares>0 && shares*p+orderFees(shares,p,side==='long'?'buy':'sell',fees)>budget+1e-9) shares--;

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
            const borrow = 0; // accrued by actual calendar days before covers
            fee = orderFees(shares, p, "sell", fees) + borrow;
            cash += notional - fee;
            lot = { ticker: t, shares, entry_px: p, entry_date: date, cost: fee, fee_in: fee, notional, last_px: p, peak_px: p, reason };
          }
          lot.borrow_annual = rate;
          pos[t] = lot;
          const recT = {
            date, ticker: t, side: side === "long" ? "BUY" : "SHORT",
            shares, price: p, fees: fee,
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
      n_trades: trades.filter(t => t.side !== "OPEN" && t.side !== "CLOSE" && t.side !== "BORROW").length,
      n_skips: skips.length,
    };
  }

  global.FMSim = {
    matches, pickDay, rankScore, lookDay, holdReturn, holdFill, simulateBook, orderFees,
    matchWhy, decisionWhy, hitTally, packRets, packPolarity,
    recipeSide, takeBucket, polarityPredict, polarityHit,
    sessionHasClosed, dateHasClose, lastClosedDate,
    ydayRet, ydayUp, majorCatalyst, whiteHorizonOverlay, whiteYdayOverlay, camGood, camBad,
  };
})(typeof window !== "undefined" ? window : globalThis);
