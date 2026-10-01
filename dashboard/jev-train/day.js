/* Train-by-day hook. Chips load that day's parse from the repo. No token. */
(function (root, factory) {
  var api = factory();
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  if (root) root.JevTrainDay = api;
  if (typeof document !== "undefined") {
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", api.boot);
    else api.boot();
  }
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  var REPO = "SRoyaltyy/fullscan";
  var WORKFLOW = "jev_train.yml";
  var RAW_DAYS = "https://raw.githubusercontent.com/" + REPO + "/main/dashboard/jev-train/days.json";
  var RAW_NEWS = "https://raw.githubusercontent.com/" + REPO + "/main/01_daily/news/";
  var DAY_CAP = 500;
  var daysCache = [];

  function tokenValue() {
    var el = document.getElementById("token");
    return el ? String(el.value || "").trim() : "";
  }

  function setErr(text) {
    var el = document.getElementById("err");
    if (el) el.textContent = text || "";
  }

  function setMeta(text) {
    var el = document.getElementById("meta");
    if (el) el.textContent = text || "";
  }

  function currentDay() {
    if (window.JevTrain && window.JevTrain.getFilterDay) return window.JevTrain.getFilterDay() || "";
    var pick = document.getElementById("dayPick");
    return pick ? String(pick.value || "") : "";
  }

  function newsUrls(day, info) {
    var names = [];
    function add(name) {
      if (names.indexOf(name) < 0) names.push(name);
    }
    if (!info || info.parsed) add(day + "_parsed.json");
    if (!info || info.digest) {
      add(day + "_finviz_digest.json");
      add(day + "_finviz_market_digest.json");
    }
    if (!info || info.judge) add(day + "_judge.json");
    add(day + "_parsed.json");
    add(day + "_finviz_digest.json");
    add(day + "_finviz_market_digest.json");
    add(day + "_judge.json");
    var out = [];
    names.forEach(function (name) {
      out.push(RAW_NEWS + name + "?t=" + Date.now());
      out.push("../../01_daily/news/" + name);
    });
    return out;
  }

  function rowsFromNews(blob, day, kind, cap) {
    cap = cap || DAY_CAP;
    var raw = [];
    if (kind === "parsed") {
      raw = (blob && blob.all_items) || [];
    } else if (kind === "digest") {
      raw = [].concat((blob && blob.top_signal) || [], (blob && blob.index_digests) || []);
    } else if (kind === "judge") {
      raw = ((blob && blob.top_items) || []).map(function (text) {
        return { title: String(text || "").split(" | ", 1)[0] };
      });
      var extra = String((blob && blob.rescued_from_noise) || "");
      extra.split(";").forEach(function (part) {
        if (part.trim()) raw.push({ title: part.trim() });
      });
    }
    var seen = {};
    var out = [];
    raw.forEach(function (it) {
      if (!it || typeof it !== "object") return;
      var title = String(it.title || it.news_title || it.digest || "").trim().slice(0, 400);
      if (!title || seen[title]) return;
      seen[title] = 1;
      out.push({
        n: out.length + 1,
        id: "p" + day.replace(/-/g, "") + "-" + out.length,
        pool: kind || "day",
        title: title,
        source: String(it.source || (kind === "digest" ? "finviz_digest" : "")).slice(0, 160),
        published_at: String(it.published_at || day),
        url: String(it.url || "").slice(0, 400),
        date: day,
        jev: "",
        reason: "",
        bits: [],
        geo: "",
        actor_power: "",
        new_instrument: 0
      });
    });
    return out.slice(0, cap);
  }

  function kindFromUrl(url) {
    if (url.indexOf("_parsed.json") !== -1) return "parsed";
    if (url.indexOf("digest") !== -1) return "digest";
    if (url.indexOf("_judge.json") !== -1) return "judge";
    return "day";
  }

  function previewDraw(day, items, kind, total) {
    return {
      schema: "jev-train-draw-1",
      stamp: "preview-" + day,
      preview: true,
      day: day,
      day_kind: kind,
      day_n: total || items.length,
      exam_source: "day:" + day + ":" + kind,
      gate: "repo-parse",
      items: items
    };
  }

  function paintChips() {
    var box = document.getElementById("dayChips");
    var list = document.getElementById("dayList");
    if (!box) return;
    box.replaceChildren();
    var selected = currentDay();
    var all = document.createElement("button");
    all.type = "button";
    all.textContent = "All dates";
    all.className = selected ? "" : "on";
    all.addEventListener("click", function () { applyFilter(""); });
    box.appendChild(all);
    daysCache.forEach(function (row) {
      var day = row.date || row;
      var btn = document.createElement("button");
      btn.type = "button";
      btn.dataset.day = day;
      btn.className = selected === day ? "on" : "";
      var flags = [];
      if (row.parsed) flags.push("parsed");
      if (row.digest) flags.push("digest");
      if (row.judge) flags.push("judge");
      btn.appendChild(document.createTextNode(String(day).slice(5)));
      if (flags.length) {
        var kind = document.createElement("span");
        kind.className = "kind";
        kind.textContent = flags[0][0];
        btn.appendChild(kind);
      }
      btn.title = day + (flags.length ? " " + flags.join("/") : "");
      btn.addEventListener("click", function () { applyFilter(day); });
      box.appendChild(btn);
    });
    if (list) {
      list.replaceChildren();
      daysCache.forEach(function (row) {
        var opt = document.createElement("option");
        opt.value = row.date || row;
        list.appendChild(opt);
      });
    }
  }

  async function loadDays() {
    var urls = [RAW_DAYS + "?t=" + Date.now(), "days.json?t=" + Date.now()];
    for (var i = 0; i < urls.length; i++) {
      try {
        var res = await fetch(urls[i], { cache: "no-store" });
        if (!res.ok) continue;
        var data = await res.json();
        daysCache = (data && data.days) || [];
        paintChips();
        return;
      } catch (err) { /* try next */ }
    }
  }

  function dayInfo(day) {
    for (var i = 0; i < daysCache.length; i++) {
      if ((daysCache[i].date || daysCache[i]) === day) return daysCache[i];
    }
    return null;
  }

  async function loadParseDay(day) {
    var urls = newsUrls(day, dayInfo(day));
    var errors = [];
    for (var i = 0; i < urls.length; i++) {
      try {
        var res = await fetch(urls[i], { cache: "no-store" });
        if (!res.ok) {
          errors.push(res.status + " " + urls[i].split("?")[0]);
          continue;
        }
        var blob = await res.json();
        var kind = kindFromUrl(urls[i]);
        var items = rowsFromNews(blob, day, kind, DAY_CAP);
        if (!items.length) continue;
        return previewDraw(day, items, kind, items.length);
      } catch (err) {
        errors.push(String(err && err.message ? err.message : err));
      }
    }
    throw new Error("No parse/digest/judge titles for " + day + (errors.length ? " (" + errors[0] + ")" : ""));
  }

  function applyFilter(day) {
    var pick = document.getElementById("dayPick");
    if (pick) pick.value = day || "";
    paintChips();
    if (!day) {
      if (window.JevTrain && window.JevTrain.setFilterDay) window.JevTrain.setFilterDay("");
      else if (window.JevTrain && window.JevTrain.reloadDraw) window.JevTrain.reloadDraw();
      return Promise.resolve();
    }
    setErr("");
    setMeta("Loading " + day + " parse from the repo…");
    return loadParseDay(day).then(function (draw) {
      if (window.JevTrain && window.JevTrain.showLocalDraw) window.JevTrain.showLocalDraw(draw);
      else if (window.JevTrain && window.JevTrain.setFilterDay) window.JevTrain.setFilterDay(day);
    }).catch(function (err) {
      if (window.JevTrain && window.JevTrain.setFilterDay) window.JevTrain.setFilterDay(day);
      setErr(String(err && err.message ? err.message : err));
    });
  }

  async function dispatchDay(day) {
    var token = tokenValue();
    if (!token) throw new Error("Paste a GitHub token with workflow scope.");
    var res = await fetch("https://api.github.com/repos/" + REPO + "/actions/workflows/" + WORKFLOW + "/dispatches", {
      method: "POST",
      headers: {
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
        Authorization: "Bearer " + token
      },
      body: JSON.stringify({
        ref: "main",
        inputs: { mode: "draw", stamp: "", grades_json: "", seed: "", day: day }
      })
    });
    if (res.status === 204) return;
    var text = await res.text();
    throw new Error("GitHub " + res.status + " " + text.slice(0, 240));
  }

  function boot() {
    loadDays();
    var filterBtn = document.getElementById("filterDay");
    if (filterBtn) {
      filterBtn.addEventListener("click", function () {
        var day = (document.getElementById("dayPick") || {}).value || "";
        if (day && !/^\d{4}-\d{2}-\d{2}$/.test(day)) {
          setErr("Pick a YYYY-MM-DD date first.");
          return;
        }
        setErr("");
        applyFilter(day);
      });
    }
    var clearBtn = document.getElementById("clearDay");
    if (clearBtn) clearBtn.addEventListener("click", function () { applyFilter(""); });
    var btn = document.getElementById("loadDay");
    if (btn) {
      btn.addEventListener("click", function () {
        var day = (document.getElementById("dayPick") || {}).value || "";
        if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) {
          setErr("Pick a YYYY-MM-DD date first.");
          return;
        }
        setErr("");
        applyFilter(day);
        setMeta("Day " + day + " dispatched (live JEV when the key is available). When the action finishes, click Reload draw.");
        dispatchDay(day).catch(function (err) {
          setErr(String(err && err.message ? err.message : err));
        });
      });
    }
  }

  return {
    DAY_CAP: DAY_CAP,
    newsUrls: newsUrls,
    rowsFromNews: rowsFromNews,
    previewDraw: previewDraw,
    paintChips: paintChips,
    loadDays: loadDays,
    applyFilter: applyFilter,
    boot: boot
  };
});
