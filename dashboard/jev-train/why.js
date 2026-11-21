/* Relabel the why-bits column on the trainer page.
   Keep iff (done OR print OR spoke) AND NOT tip.
   Old mapped tags (signed/lever/earnings/blast) are not shown.
   Match rows by id or title so a stale Pages draw.json cannot win. */
(function () {
  var SIX = ["tape", "soft", "tip", "done", "print", "spoke"];
  var NOUL = 0.60;
  var lastDraw = null;

  function paintWhy(row) {
    var reason = String((row && row.reason) || "").trim();
    if (row && row.signals) {
      return reason + " · " + Object.keys(row.signals).map(function (key) {
        return key + ": " + Number(row.signals[key]).toFixed(2);
      }).join(" · ");
    }
    var noul = (row && row.noul) || {};
    var on = [];
    for (var i = 0; i < SIX.length; i++) {
      var name = SIX[i];
      var score = Number(noul[name]);
      if (isFinite(score) && score >= NOUL) on.push(name);
    }
    if (!on.length && row && Array.isArray(row.bits)) {
      for (var j = 0; j < SIX.length; j++) {
        if (row.bits.indexOf(SIX[j]) !== -1) on.push(SIX[j]);
      }
    }
    if (reason && SIX.indexOf(reason) !== -1 && on.indexOf(reason) === -1) {
      on.unshift(reason);
    }
    if (reason && SIX.indexOf(reason) !== -1) return on.join(" ") || reason;
    if (reason && reason !== "earnings" && reason !== "signed" && reason !== "lever" && reason !== "blast" && reason !== "actor" && reason !== "opinion") {
      return on.length ? (reason + " \u00b7 " + on.join(" ")) : reason;
    }
    return on.join(" ") || "\u2014";
  }

  function relabel() {
    if (!lastDraw || !lastDraw.items) return;
    var byId = {};
    var byTitle = {};
    lastDraw.items.forEach(function (row) {
      if (!row) return;
      if (row.id) byId[row.id] = row;
      if (row.title) byTitle[row.title] = row;
    });
    document.querySelectorAll("#sheet tbody tr").forEach(function (tr) {
      var titleEl = tr.querySelector("td.title");
      var title = titleEl ? titleEl.textContent : "";
      var row = byId[tr.dataset.id] || byTitle[title];
      var td = tr.querySelector("td.bits");
      if (row && td) td.textContent = tr.dataset.blind === "true" ? "" : paintWhy(row);
    });
  }

  function remember(draw) {
    if (!draw || !Array.isArray(draw.items)) return;
    if (!lastDraw || String(draw.stamp || "") >= String(lastDraw.stamp || "")) {
      lastDraw = draw;
    }
    relabel();
  }

  function hookApi() {
    var api = window.JevTrain;
    if (!api || api.__sixBitWhy) return;
    api.__sixBitWhy = true;
    api.paintWhy = paintWhy;
    var show = api.showLocalDraw;
    if (typeof show === "function") {
      api.showLocalDraw = function (draw) {
        var out = show.apply(this, arguments);
        remember(draw);
        return out;
      };
    }
    var tbody = document.querySelector("#sheet tbody");
    if (tbody) {
      new MutationObserver(function () { relabel(); }).observe(tbody, { childList: true });
    }
  }

  function loadDraw() {
    fetch("https://raw.githubusercontent.com/SRoyaltyy/fullscan/codex/jev-lane-hop0/dashboard/jev-train/draw.json?t=" + Date.now(), { cache: "no-store" })
      .then(function (res) { return res.ok ? res.json() : null; })
      .then(remember)
      .catch(function () {});
  }

  function boot() {
    hookApi();
    loadDraw();
    relabel();
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
  setTimeout(boot, 0);
  setTimeout(boot, 400);
  setTimeout(loadDraw, 800);
})();
