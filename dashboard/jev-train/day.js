/* Train-by-day hook. Keeps mixed-draw app.js intact. */
(function () {
  var REPO = "SRoyaltyy/fullscan";
  var WORKFLOW = "jev_train.yml";
  var RAW_DAYS = "https://raw.githubusercontent.com/" + REPO + "/main/dashboard/jev-train/days.json";
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

  function applyFilter(day) {
    if (window.JevTrain && window.JevTrain.setFilterDay) window.JevTrain.setFilterDay(day || "");
    else {
      var pick = document.getElementById("dayPick");
      if (pick) pick.value = day || "";
    }
    paintChips();
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
      btn.appendChild(document.createTextNode(day.slice(5)));
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

  document.addEventListener("DOMContentLoaded", function () {
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
        setMeta("Day " + day + " dispatched. When the action finishes, click Reload draw.");
        dispatchDay(day).catch(function (err) {
          setErr(String(err && err.message ? err.message : err));
        });
      });
    }
  });

  window.JevTrainDay = { paintChips: paintChips, loadDays: loadDays };
})();
