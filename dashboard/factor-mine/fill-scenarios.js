/* Fill-reality scenarios on the existing strategy display.
   Research only — does not change live flatten_robust / hard-red sit / Webull.
   Market buy: wrong-price / partial / missed. Limit buy: fill-or-miss. */
(function () {
  "use strict";

  var MARKET = [
    { key: "ideal", label: "published 09:30", kind: "baseline" },
    { key: "market_mid", label: "market · wrong price (α delay)", kind: "wrong_price" },
    { key: "market_adverse", label: "market · worst print", kind: "wrong_price" },
    { key: "market_favorable", label: "market · best print (unlikely)", kind: "wrong_price" },
    { key: "partial_50", label: "market · partial 50%", kind: "partial" },
    { key: "gap_miss", label: "market · missed fill (gap ≥8%)", kind: "miss" },
  ];
  var LIMIT = [
    { key: "ideal", label: "published 09:30", kind: "baseline" },
    { key: "limit_open", label: "limit @ open (fill or miss)", kind: "miss" },
    { key: "limit_prior", label: "limit @ prior close (fill or miss)", kind: "miss" },
    { key: "gap_miss", label: "limit · missed fill (gap ≥8%)", kind: "miss" },
  ];

  var DATA = null;
  var URLS = [
    "book_fill_reality.json",
    "./book_fill_reality.json",
    "../factor-mine/book_fill_reality.json",
    "../../03_scoreboard/book_fill_reality.json",
  ];

  function esc(s) {
    if (typeof window.esc === "function") return window.esc(s);
    return String(s == null ? "" : s).replace(/[&<>"]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
    });
  }
  function num(v, d) {
    if (typeof window.num === "function") return window.num(v, d);
    if (v == null || v === "") return "—";
    var x = Number(v);
    return (x >= 0 ? "+" : "") + x.toFixed(d == null ? 2 : d);
  }
  function pct(v) {
    if (typeof window.pct === "function") return window.pct(v);
    return v == null ? "—" : (100 * Number(v)).toFixed(0) + "%";
  }
  function cls(v) {
    if (typeof window.cls === "function") return window.cls(v);
    return v == null ? "mut" : Number(v) >= 0 ? "pos" : "neg";
  }
  function fillMode() {
    return String(window.__FILL_MODE || "");
  }
  function specsFor(mode) {
    return mode === "limit" ? LIMIT : MARKET;
  }
  function inferMeta(name) {
    var holdM = String(name || "").match(/_h(\d+)\b/);
    var side = "long";
    if (/^short_/.test(name) || /_short/.test(name)) side = "short";
    if (/^combo_/.test(name)) side = "mix";
    return { side: side, hold: holdM ? Number(holdM[1]) : "—" };
  }
  function reportOf(name) {
    if (!DATA) return null;
    var reps = DATA.reports || {};
    if (reps[name]) return reps[name];
    if (name === "flatten_would" && reps.flatten_h3) return reps.flatten_h3;
    return null;
  }
  function dayRate(st) {
    var n = Number(st.n_sessions || 0);
    if (!n) return null;
    return Number(st.n_sessions_green || 0) / n;
  }
  function winRate(st) {
    if (st.win_session_pct == null) return null;
    return Number(st.win_session_pct) / 100;
  }
  function startRate(st) {
    if (st.start_n == null || !Number(st.start_n)) return null;
    return Number(st.start_green || 0) / Number(st.start_n);
  }
  function countsLine(st) {
    return (st.n_fills || 0) + " fills · " + (st.n_miss || 0) + " miss · " +
      (st.n_partial || 0) + " partial";
  }

  function injectStyle() {
    if (document.getElementById("fillScenStyle")) return;
    var s = document.createElement("style");
    s.id = "fillScenStyle";
    s.textContent =
      ".fill-tabs{display:flex;flex-wrap:wrap;gap:6px;margin:8px 0}" +
      ".fill-tabs button,.fill-chip{cursor:pointer;padding:7px 12px;border-radius:999px;" +
      "border:1px solid #2a3450;background:#171e2e;color:#aeb9cf;font:12px/1.3 ui-monospace,Menlo,Consolas,monospace}" +
      ".fill-tabs button.on,.fill-chip.on{background:#dfe6f2;color:#0f1420;border-color:#dfe6f2}" +
      "tr.scen td.tick,tr.scen td.name{padding-left:16px}" +
      ".scen-lab{color:#8b96ab;font-size:11px;margin-top:2px}" +
      "select#fillPick{min-width:220px}";
    document.head.appendChild(s);
  }

  function setFillMode(mode, rerender) {
    window.__FILL_MODE = mode || "";
    try { sessionStorage.setItem("fmFillMode", window.__FILL_MODE); } catch (e) {}
    document.querySelectorAll("#fillPick").forEach(function (el) { el.value = window.__FILL_MODE; });
    document.querySelectorAll("[data-fill-mode]").forEach(function (el) {
      el.classList.toggle("on", (el.getAttribute("data-fill-mode") || "") === window.__FILL_MODE);
    });
    if (rerender === false) return;
    if (typeof window.renderAll === "function") window.renderAll();
    else {
      if (typeof window.renderCards === "function") window.renderCards();
      if (typeof window.renderTable === "function") window.renderTable();
    }
    paintOverlay();
    paintStrategyBoard();
    paintStandalone();
  }

  function injectFillPicker() {
    var bar = document.getElementById("filterBar");
    if (!bar || document.getElementById("fillPick")) return;
    var sel = document.createElement("select");
    sel.id = "fillPick";
    sel.setAttribute("aria-label", "Fill reality");
    sel.innerHTML =
      '<option value="">Published 09:30 fills</option>' +
      '<option value="market">Market-buy messiness</option>' +
      '<option value="limit">Limit-buy messiness</option>';
    sel.value = fillMode();
    sel.onchange = function (e) { setFillMode(e.target.value); };
    bar.insertBefore(sel, bar.firstChild);
  }

  function scenarioPairs(stat, mode) {
    var report = reportOf(stat.name);
    if (!report) return [];
    var cols = report.columns || {};
    return specsFor(mode).map(function (spec) {
      var st = cols[spec.key];
      return st ? { spec: spec, st: st, stat: stat } : null;
    }).filter(Boolean);
  }

  function renderScenarioTable() {
    var host = document.getElementById("stats");
    if (!host) return;
    var mode = fillMode();
    var q = ((document.getElementById("q") || {}).value || "").trim().toLowerCase();
    var list = typeof window.statsList === "function" ? window.statsList() : [];
    if (q) {
      list = list.filter(function (s) {
        return String(s.name || "").toLowerCase().indexOf(q) >= 0 ||
          String(s.note || "").toLowerCase().indexOf(q) >= 0;
      });
    }
    if (typeof sleeveFilter !== "undefined" && sleeveFilter !== "all") {
      list = list.filter(function (s) { return s.name === sleeveFilter; });
    }
    var head = "<tr><th class=\"tick\">Strategy</th><th>Side</th><th>H</th><th>Win%</th>" +
      "<th>$ days</th><th>Starts</th><th>Book%</th><th>Signal%</th><th>Audit</th></tr>";
    var body = list.map(function (s) {
      var pairs = scenarioPairs(s, mode);
      if (!pairs.length) {
        return oneFactorRow(s, null, null, false);
      }
      return pairs.map(function (p, i) {
        return oneFactorRow(s, p.spec, p.st, i > 0);
      }).join("");
    }).join("");
    host.innerHTML = head + body;
    host.querySelectorAll("tr[data-sleeve]").forEach(function (tr) {
      tr.onclick = function () {
        if (typeof window.selectSleeve === "function") {
          window.selectSleeve(tr.dataset.sleeve);
        }
      };
    });
  }

  function oneFactorRow(s, spec, st, child) {
    var book = spec ? st.book_pct : s.total_ret_pct;
    var win = spec ? winRate(st) : s.win_rate;
    var days = spec ? dayRate(st) : s.profitable_day_rate;
    var sg = spec ? st.start_green : s.start_green;
    var sn = spec ? st.start_n : s.start_n;
    var starts = (sg == null || sn == null) ? "—" : (sg + "/" + sn);
    var startCls = spec ? cls(startRate(st)) : cls(s.start_rate);
    var auditOk = spec ? st.audit_ok !== false : s.audit_ok !== false;
    var meta = inferMeta(s.name);
    var side = s.side || meta.side;
    var hold = s.hold != null ? s.hold : meta.hold;
    var lab = spec ? spec.label : "published 09:30";
    var extra = spec ? countsLine(st) : (s.size || "leftover") + " · " + (s.sell || "list") + " · " + (s.s_boost || "none");
    var legs = (!spec && typeof window.legsLine === "function") ? window.legsLine(s) : "";
    return "<tr data-sleeve=\"" + esc(s.name) + "\" class=\"" + (child ? "scen" : "") + "\" style=\"cursor:pointer\">" +
      "<td class=\"tick\">" + esc(s.name) +
      (s.reliable === false && !spec ? " <span class=\"mut\">thin</span>" : "") +
      (s.outperforms && !spec ? " <span class=\"pos\">WIN</span>" : "") +
      "<div class=\"scen-lab\">" + esc(lab) + "</div>" +
      "<div class=\"mut\">" + esc(extra) + "</div>" + legs + "</td>" +
      "<td>" + esc(side) + "</td>" +
      "<td class=\"num\">" + esc(hold) + "</td>" +
      "<td class=\"num " + cls(win) + "\">" + pct(win) + "</td>" +
      "<td class=\"num " + cls(days) + "\">" + pct(days) + "</td>" +
      "<td class=\"num " + startCls + "\">" + starts + "</td>" +
      "<td class=\"num " + cls(book) + "\">" + num(book) + "</td>" +
      "<td class=\"num mut\">" + (spec ? "—" : num(s.signal_ret_pct)) + "</td>" +
      "<td class=\"" + (auditOk ? "pos" : "neg") + "\">" + (auditOk ? "PASS" : "FAIL") + "</td>" +
      "</tr>";
  }

  function renderScenarioCards() {
    var host = document.getElementById("cards");
    if (!host) return;
    var mode = fillMode();
    var list = typeof window.statsList === "function" ? window.statsList() : [];
    var pickedName = (typeof sleeveFilter !== "undefined" && sleeveFilter !== "all") ? sleeveFilter : "";
    var spotlight = pickedName || (DATA && DATA.spotlight) || "combo_sh_5050_shared";
    var stat = list.filter(function (s) { return s.name === spotlight; })[0] ||
      { name: spotlight, side: inferMeta(spotlight).side, hold: inferMeta(spotlight).hold };
    var pairs = scenarioPairs(stat, mode);
    if (!pairs.length) {
      if (typeof ORIG.cards === "function") ORIG.cards();
      return;
    }
    host.innerHTML = pairs.map(function (p) {
      var book = p.st.book_pct;
      var c = cls(book);
      var on = pickedName === stat.name ? "on" : "";
      return "<div class=\"card " + on + "\" data-sleeve=\"" + esc(stat.name) + "\">" +
        "<span class=\"mut\">" + esc(p.spec.label) + "</span>" +
        "<b class=\"" + c + "\">" + num(book) + "%</b>" +
        "<div class=\"mut\">" + esc(stat.name) + "</div>" +
        "<div class=\"scen-lab\">$ days " + pct(dayRate(p.st)) +
        " · starts " + (p.st.start_green == null ? "—" : (p.st.start_green + "/" + p.st.start_n)) +
        " · " + esc(countsLine(p.st)) + "</div></div>";
    }).join("");
    host.querySelectorAll(".card[data-sleeve]").forEach(function (el) {
      el.onclick = function () {
        if (typeof window.selectSleeve === "function") {
          window.selectSleeve(sleeveFilter === el.dataset.sleeve ? "all" : el.dataset.sleeve);
        }
      };
    });
  }

  function strategyTableHtml(mode, names) {
    var reps = (DATA && DATA.reports) || {};
    names = names || (DATA && DATA.sleeves) || Object.keys(reps);
    var head = "<tr><th class=\"tick\">Strategy</th><th>Side</th><th>H</th><th>Win%</th>" +
      "<th>$ days</th><th>Starts</th><th>Book%</th><th>Signal%</th><th>Audit</th></tr>";
    var body = [];
    names.forEach(function (name) {
      var report = reportOf(name);
      if (!report) return;
      var cols = report.columns || {};
      var meta = inferMeta(name);
      specsFor(mode).forEach(function (spec, i) {
        var st = cols[spec.key];
        if (!st) return;
        var win = winRate(st);
        var days = dayRate(st);
        var book = st.book_pct;
        var starts = (st.start_green == null || st.start_n == null) ? "—" :
          (st.start_green + "/" + st.start_n);
        body.push(
          "<tr class=\"" + (i ? "scen" : "") + "\">" +
          "<td class=\"tick\">" + esc(name) +
          "<div class=\"scen-lab\">" + esc(spec.label) + "</div>" +
          "<div class=\"mut\">" + esc(countsLine(st)) + "</div></td>" +
          "<td>" + esc(meta.side) + "</td>" +
          "<td class=\"num\">" + esc(meta.hold) + "</td>" +
          "<td class=\"num " + cls(win) + "\">" + pct(win) + "</td>" +
          "<td class=\"num " + cls(days) + "\">" + pct(days) + "</td>" +
          "<td class=\"num " + cls(startRate(st)) + "\">" + starts + "</td>" +
          "<td class=\"num " + cls(book) + "\">" + num(book) + "</td>" +
          "<td class=\"num mut\">—</td>" +
          "<td class=\"" + (st.audit_ok === false ? "neg" : "pos") + "\">" +
          (st.audit_ok === false ? "FAIL" : "PASS") + "</td></tr>"
        );
      });
    });
    return "<div class=\"tbl-wrap\"><table>" + head + body.join("") + "</table></div>";
  }

  function tabsHtml(active) {
    return "<div class=\"fill-tabs\" role=\"tablist\">" +
      "<button type=\"button\" data-fill-mode=\"market\" class=\"" + (active === "market" ? "on" : "") + "\">Market buy</button>" +
      "<button type=\"button\" data-fill-mode=\"limit\" class=\"" + (active === "limit" ? "on" : "") + "\">Limit buy</button>" +
      "</div>";
  }

  function paintOverlay() {
    var host = document.getElementById("bookFillRealityBody");
    if (!host || !DATA) return;
    var mode = fillMode() || "market";
    if (mode !== "limit") mode = "market";
    var names = DATA.sleeves || Object.keys(DATA.reports || {});
    var featured = names.filter(function (n) {
      var r = reportOf(n) || {};
      return r.must_run || r.featured;
    });
    if (!featured.length) featured = names.slice(0, 12);
    host.innerHTML =
      "<p>" + esc(DATA.headline || DATA.note || "") + "</p>" +
      "<p class=\"mut\">Same columns as the strategy table. Market = wrong-price / partial / missed. " +
      "Limit = working order at the open or prior close (fill or miss). " +
      "Use the <b>Market-buy</b> / <b>Limit-buy</b> filter on the main table to expand every sleeve.</p>" +
      tabsHtml(mode) +
      strategyTableHtml(mode, featured);
    host.querySelectorAll("[data-fill-mode]").forEach(function (btn) {
      btn.onclick = function () { setFillMode(btn.getAttribute("data-fill-mode")); };
    });
  }

  function paintStandalone() {
    var host = document.getElementById("fillScenarioRoot");
    if (!host || !DATA) return;
    var mode = fillMode() || "market";
    if (mode !== "limit") mode = "market";
    var names = DATA.sleeves || Object.keys(DATA.reports || {});
    host.innerHTML =
      tabsHtml(mode) +
      "<p class=\"mut\">Strategy · Side · H · Win% · $ days · Starts · Book% · Signal% · Audit — " +
      "the same columns as the factor-mine strategy table. Signal% is the published equal-weight path and stays on the ideal row only.</p>" +
      strategyTableHtml(mode, names);
    host.querySelectorAll("[data-fill-mode]").forEach(function (btn) {
      btn.onclick = function () { setFillMode(btn.getAttribute("data-fill-mode")); };
    });
  }

  function ensureStrategyChips() {
    var chips = document.getElementById("chips");
    if (!chips || document.getElementById("fillChips")) return;
    var row = document.createElement("div");
    row.id = "fillChips";
    row.className = "chips";
    row.style.marginTop = "8px";
    row.innerHTML =
      "<button type=\"button\" data-fill-mode=\"\" class=\"fill-chip on\">published fills</button>" +
      "<button type=\"button\" data-fill-mode=\"market\" class=\"fill-chip\">Market buy</button>" +
      "<button type=\"button\" data-fill-mode=\"limit\" class=\"fill-chip\">Limit buy</button>";
    chips.insertAdjacentElement("afterend", row);
    row.querySelectorAll("[data-fill-mode]").forEach(function (btn) {
      btn.onclick = function () { setFillMode(btn.getAttribute("data-fill-mode")); };
    });
  }

  function paintStrategyBoard() {
    var table = document.getElementById("T");
    if (!table || !DATA) return;
    ensureStrategyChips();
    table.querySelectorAll("tr.scen").forEach(function (tr) { tr.remove(); });
    var mode = fillMode();
    if (!mode) return;
    var tbody = table.tBodies[0] || table;
    Array.prototype.slice.call(tbody.querySelectorAll("tr[data-sleeve]")).forEach(function (tr) {
      var name = tr.getAttribute("data-sleeve");
      var report = reportOf(name);
      if (!report) return;
      var cols = report.columns || {};
      var html = "";
      specsFor(mode).forEach(function (spec, i) {
        if (spec.key === "ideal") return;
        var st = cols[spec.key];
        if (!st) return;
        var book = st.book_pct;
        var win = winRate(st);
        var rcls = book == null ? "" : (Number(book) > 0 ? "good" : (Number(book) < 0 ? "bad" : ""));
        html += "<tr class=\"scen\" data-family=\"" + esc(tr.getAttribute("data-family") || "factor mine") + "\" " +
          "data-integrity=\"" + esc(tr.getAttribute("data-integrity") || "fill") + "\" data-sleeve=\"" + esc(name) + "\">" +
          "<td class=\"name\">" + esc(name) +
          "<div class=\"scen-lab\">" + esc(spec.label) + "</div>" +
          "<div class=\"muted\">" + esc(countsLine(st)) + "</div></td>" +
          "<td>factor mine</td><td>—</td>" +
          "<td class=\"tag fill\">fill</td>" +
          "<td class=\"" + rcls + "\">" + (book == null ? "—" : num(book) + "%") + "</td>" +
          "<td>" + (st.max_dd_pct == null ? "—" : Number(st.max_dd_pct).toFixed(2) + "%") + "</td>" +
          "<td>" + (st.n_fills == null ? "—" : st.n_fills) + "</td>" +
          "<td>" + (win == null ? "—" : (100 * win).toFixed(1) + "%") + "</td>" +
          "<td>$10,000</td>" +
          "<td class=\"why\">" + esc(spec.label) + " · leftover-cash butterfly</td></tr>";
      });
      if (html) tr.insertAdjacentHTML("afterend", html);
    });
  }

  var ORIG = {};
  function wrapFactorMine() {
    if (typeof window.renderTable !== "function") return false;
    if (window.__FILL_SCEN_WRAPPED) return true;
    window.__FILL_SCEN_WRAPPED = true;
    ORIG.table = window.renderTable;
    ORIG.cards = window.renderCards;
    ORIG.filters = window.renderFilters;
    window.renderTable = function () {
      if (!fillMode() || !DATA) { ORIG.table(); return; }
      renderScenarioTable();
    };
    if (typeof window.renderCards === "function") {
      window.renderCards = function () {
        if (!fillMode() || !DATA) { ORIG.cards(); return; }
        renderScenarioCards();
      };
    }
    if (typeof window.renderFilters === "function") {
      window.renderFilters = function () {
        ORIG.filters();
        injectFillPicker();
      };
    }
    injectFillPicker();
    return true;
  }

  function bootWhenReady() {
    injectStyle();
    var n = 0;
    (function wait() {
      if (wrapFactorMine() || document.getElementById("T") || document.getElementById("fillScenarioRoot") || n > 80) {
        injectFillPicker();
        paintOverlay();
        paintStrategyBoard();
        paintStandalone();
        if (fillMode() && typeof window.renderAll === "function") window.renderAll();
        return;
      }
      n += 1;
      setTimeout(wait, 50);
    })();
  }

  function load(i) {
    if (i >= URLS.length) {
      var host = document.getElementById("bookFillRealityBody");
      if (host && /loading/i.test(host.textContent || "")) {
        host.textContent = "book-fill reality JSON not on this deploy yet. Run python -m src.book_fill_reality --write.";
      }
      bootWhenReady();
      return;
    }
    fetch(URLS[i], { cache: "no-store" }).then(function (r) {
      if (!r.ok) throw 0;
      return r.json();
    }).then(function (d) {
      DATA = d;
      window.__FILL_REALITY = d;
      try {
        var saved = sessionStorage.getItem("fmFillMode");
        if (saved) window.__FILL_MODE = saved;
      } catch (e) {}
      bootWhenReady();
    }).catch(function () { load(i + 1); });
  }

  window.FillScenarios = {
    MARKET: MARKET,
    LIMIT: LIMIT,
    setFillMode: setFillMode,
    strategyTableHtml: function (mode) { return strategyTableHtml(mode); },
  };
  load(0);
})();
