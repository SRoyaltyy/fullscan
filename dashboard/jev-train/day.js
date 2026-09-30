/* Train-by-day hook. Keeps mixed-draw app.js intact. */
(function () {
  var REPO = "SRoyaltyy/fullscan";
  var WORKFLOW = "jev_train.yml";
  var RAW_DAYS = "https://raw.githubusercontent.com/" + REPO + "/main/dashboard/jev-train/days.json";

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

  async function loadDays() {
    var list = document.getElementById("dayList");
    if (!list) return;
    var urls = [RAW_DAYS + "?t=" + Date.now(), "days.json?t=" + Date.now()];
    for (var i = 0; i < urls.length; i++) {
      try {
        var res = await fetch(urls[i], { cache: "no-store" });
        if (!res.ok) continue;
        var data = await res.json();
        var days = (data && data.days) || [];
        list.replaceChildren();
        days.forEach(function (row) {
          var opt = document.createElement("option");
          opt.value = row.date || row;
          var flags = [];
          if (row.parsed) flags.push("parsed");
          if (row.digest) flags.push("digest");
          if (row.judge) flags.push("judge");
          opt.label = flags.length ? opt.value + " " + flags.join("/") : opt.value;
          list.appendChild(opt);
        });
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
    var btn = document.getElementById("loadDay");
    if (!btn) return;
    btn.addEventListener("click", function () {
      var day = (document.getElementById("dayPick") || {}).value || "";
      if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) {
        setErr("Pick a YYYY-MM-DD date first.");
        return;
      }
      setErr("");
      setMeta("Day " + day + " dispatched. When the action finishes, click Reload draw.");
      dispatchDay(day).catch(function (err) {
        setErr(String(err && err.message ? err.message : err));
      });
    });
  });
})();
