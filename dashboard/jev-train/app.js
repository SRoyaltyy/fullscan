/* Jev trainer page. No API key and no GitHub token live in this file.
   The token is typed into the password box and sent only to api.github.com. */
(function (root, factory) {
  var api = factory();
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  if (typeof document !== "undefined") api.boot();
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  var MIN_MARKS = 30;
  var REPO = "SRoyaltyy/fullscan";
  var WORKFLOW = "jev_train.yml";
  var RAW_DRAW = "https://raw.githubusercontent.com/" + REPO + "/main/dashboard/jev-train/draw.json";

  function marksReady(grades) {
    var n = 0;
    for (var i = 0; i < grades.length; i++) {
      if (grades[i] === "K" || grades[i] === "D") n += 1;
    }
    return n >= MIN_MARKS;
  }

  function buildGrades(draw, marks, nonce) {
    var items = (draw && draw.items) || [];
    var rows = items.map(function (row) {
      var mark = (marks && marks[row.id]) || { grade: "?", human_reason: "" };
      return {
        id: row.id,
        title: row.title,
        source: row.source || "",
        pool: row.pool || "",
        published_at: row.published_at || "",
        url: row.url || "",
        date: row.date || "",
        jev: row.jev,
        reason: row.reason || "",
        geo: row.geo || "",
        actor_power: row.actor_power || "",
        new_instrument: row.new_instrument || 0,
        bits: row.bits || [],
        grade: mark.grade || "?",
        human_reason: String(mark.human_reason || mark.note || "").slice(0, 500)
      };
    });
    return {
      schema: "jev-train-grades-1",
      draw_stamp: (draw && draw.stamp) || "",
      nonce: nonce || "",
      rows: rows
    };
  }

  function utf8ToB64(text) {
    var bytes = new TextEncoder().encode(text);
    var bin = "";
    for (var i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
    return btoa(bin);
  }

  function fetchTimeout(url, options, ms) {
    var ctrl = new AbortController();
    var timer = setTimeout(function () { ctrl.abort(); }, ms || 8000);
    var opts = Object.assign({}, options || {}, { signal: ctrl.signal });
    return fetch(url, opts).finally(function () { clearTimeout(timer); });
  }

  function boot() {
    var state = { draw: null, marks: {}, nonce: "", busy: false };
    var errEl = document.getElementById("err");
    var metaEl = document.getElementById("meta");
    var emptyEl = document.getElementById("empty");
    var table = document.getElementById("sheet");
    var tbody = table.querySelector("tbody");
    var countEl = document.getElementById("count");
    var submitBtn = document.getElementById("submit");
    var doneEl = document.getElementById("done");
    var issueEl = document.getElementById("issueUrl");

    function tokenValue() {
      return (document.getElementById("token").value || "").trim();
    }

    function setErr(text) {
      errEl.textContent = text || "";
    }

    function gradesList() {
      var draw = state.draw;
      if (!draw) return [];
      return draw.items.map(function (row) {
        var mark = state.marks[row.id];
        return mark ? mark.grade : "?";
      });
    }

    function markedCount() {
      return gradesList().filter(function (g) { return g === "K" || g === "D"; }).length;
    }

    function refreshSubmit() {
      var n = markedCount();
      var ready = marksReady(gradesList());
      countEl.textContent = n + " / " + MIN_MARKS + " marked K or D";
      submitBtn.disabled = !ready || state.busy || !state.draw;
      document.getElementById("download").disabled = !state.draw;
      document.getElementById("newDraw").disabled = state.busy;
    }

    function setGrade(id, grade) {
      if (!state.marks[id]) state.marks[id] = { grade: "?", human_reason: "" };
      state.marks[id].grade = grade;
      var row = tbody.querySelector('tr[data-id="' + cssEscape(id) + '"]');
      if (row) paintYou(row.querySelector(".you"), id);
      refreshSubmit();
    }

    function cssEscape(value) {
      if (window.CSS && CSS.escape) return CSS.escape(value);
      return String(value).replace(/"/g, "");
    }

    function paintYou(wrap, id) {
      var current = (state.marks[id] && state.marks[id].grade) || "?";
      wrap.querySelectorAll("button").forEach(function (button) {
        button.classList.toggle("on", button.dataset.grade === current);
      });
    }

    function render() {
      var draw = state.draw;
      tbody.replaceChildren();
      if (!draw || !draw.items || !draw.items.length) {
        table.hidden = true;
        emptyEl.hidden = false;
        metaEl.textContent = "No draw yet. New draw asks GitHub to run the hop-0 gate and write draw.json on main.";
        refreshSubmit();
        return;
      }
      emptyEl.hidden = true;
      table.hidden = false;
      var sample = draw.sample || {};
      metaEl.textContent = "Draw " + (draw.stamp || "") +
        " · exam " + (draw.exam_source || sample.exam_source || "") +
        " · parsed " + (sample.parsed != null ? sample.parsed : "") +
        " · rss " + (sample.rss != null ? sample.rss : "") +
        " · " + (draw.model || draw.gate || "");
      draw.items.forEach(function (row) {
        if (!state.marks[row.id]) state.marks[row.id] = { grade: "?", human_reason: "" };
        var tr = document.createElement("tr");
        tr.dataset.id = row.id;
        var n = document.createElement("td");
        n.className = "n";
        n.textContent = String(row.n || "");
        var title = document.createElement("td");
        title.className = "title";
        title.textContent = row.title || "";
        var source = document.createElement("td");
        source.className = "src";
        var pool = document.createElement("div");
        pool.className = "pool";
        pool.textContent = row.pool || "";
        source.appendChild(pool);
        source.appendChild(document.createTextNode(row.source || ""));
        var jev = document.createElement("td");
        jev.className = "jev " + (row.jev || "");
        jev.textContent = row.jev || "";
        var bits = document.createElement("td");
        bits.className = "bits";
        bits.textContent = (row.bits || []).join(" ");
        var youTd = document.createElement("td");
        var you = document.createElement("div");
        you.className = "you";
        ["K", "D", "?"].forEach(function (grade) {
          var button = document.createElement("button");
          button.type = "button";
          button.dataset.grade = grade;
          button.className = grade === "?" ? "Q" : grade;
          button.textContent = grade;
          button.addEventListener("click", function () { setGrade(row.id, grade); });
          you.appendChild(button);
        });
        paintYou(you, row.id);
        var reasonInput = document.createElement("input");
        reasonInput.type = "text";
        reasonInput.maxLength = 500;
        reasonInput.placeholder = "reason";
        reasonInput.setAttribute("aria-label", "reason");
        reasonInput.value = state.marks[row.id].human_reason || "";
        reasonInput.addEventListener("input", function () {
          state.marks[row.id].human_reason = reasonInput.value.replace(/\n/g, " ").slice(0, 500);
        });
        you.appendChild(reasonInput);
        youTd.appendChild(you);
        tr.append(n, title, source, jev, bits, youTd);
        tbody.appendChild(tr);
      });
      refreshSubmit();
    }

    function authHeaders() {
      var headers = {
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
      };
      var token = tokenValue();
      if (token) headers.Authorization = "Bearer " + token;
      return headers;
    }

    async function dispatch(inputs) {
      var token = tokenValue();
      if (!token) throw new Error("Paste a GitHub token with workflow scope. It stays in this tab and is not saved.");
      var res = await fetchTimeout("https://api.github.com/repos/" + REPO + "/actions/workflows/" + WORKFLOW + "/dispatches", {
        method: "POST",
        headers: Object.assign({ "Content-Type": "application/json" }, authHeaders()),
        body: JSON.stringify({ ref: "main", inputs: inputs })
      }, 20000);
      if (res.status === 204) return;
      var text = await res.text();
      if (res.status === 404) {
        throw new Error("GitHub 404. jev_train.yml has to be on main before New draw or Submit can run. Download the grades JSON and use it after that merge.");
      }
      throw new Error("GitHub " + res.status + " " + text.slice(0, 240));
    }

    function sleep(ms) {
      return new Promise(function (resolve) { setTimeout(resolve, ms); });
    }

    async function drawFromApi() {
      var headers = {
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
      };
      if (tokenValue()) headers.Authorization = "Bearer " + tokenValue();
      var res = await fetchTimeout(
        "https://api.github.com/repos/" + REPO + "/contents/dashboard/jev-train/draw.json?ref=main",
        { headers: headers, cache: "no-store" },
        8000
      );
      if (!res.ok) return null;
      var data = await res.json();
      if (!data || !data.content) return null;
      var bytes = Uint8Array.from(atob(String(data.content).replace(/\n/g, "")), function (ch) {
        return ch.charCodeAt(0);
      });
      return JSON.parse(new TextDecoder().decode(bytes));
    }

    async function loadDraw() {
      var urls = [RAW_DRAW + "?t=" + Date.now(), "draw.json?t=" + Date.now()];
      var best = null;
      var errors = [];
      try {
        var viaApi = await drawFromApi();
        if (viaApi && Array.isArray(viaApi.items)) best = viaApi;
      } catch (err) {
        errors.push(String(err));
      }
      for (var i = 0; i < urls.length; i++) {
        try {
          var res = await fetchTimeout(urls[i], { cache: "no-store" }, 8000);
          if (!res.ok) {
            errors.push(res.status + " " + urls[i]);
            continue;
          }
          var data = await res.json();
          if (!data || !Array.isArray(data.items)) continue;
          if (!best) best = data;
          else if (String(data.stamp || "") > String(best.stamp || "")) best = data;
          else if (!best.items.length && data.items.length) best = data;
        } catch (err) {
          errors.push(String(err));
        }
      }
      if (!best && errors.length) setErr(errors.join("\n"));
      return best;
    }

    async function showDraw(draw, resetMarks) {
      if (resetMarks || !state.draw || state.draw.stamp !== (draw && draw.stamp)) {
        state.marks = {};
      }
      state.draw = draw;
      render();
    }

    async function reload() {
      setErr("");
      var draw = await loadDraw();
      await showDraw(draw, false);
    }

    async function pollDraw(previous) {
      var deadline = Date.now() + 20 * 60 * 1000;
      while (Date.now() < deadline) {
        await sleep(8000);
        var draw = await loadDraw();
        if (draw && draw.items && draw.items.length && String(draw.stamp || "") !== String(previous || "")) {
          await showDraw(draw, true);
          setErr("");
          return;
        }
      }
      throw new Error("Timed out waiting for draw.json on main. The action may still be running the gate.");
    }

    async function pollIssue(nonce) {
      var deadline = Date.now() + 6 * 60 * 1000;
      while (Date.now() < deadline) {
        var res = await fetchTimeout(
          "https://api.github.com/repos/" + REPO + "/issues?labels=jev-train&state=all&per_page=20",
          { headers: authHeaders() },
          8000
        );
        if (res.ok) {
          var issues = await res.json();
          var hit = (issues || []).find(function (issue) {
            return issue && String(issue.title || "").indexOf("jev-train ") === 0 &&
              String(issue.body || "").indexOf("nonce: " + nonce) !== -1;
          });
          if (hit && hit.html_url) return hit.html_url;
        }
        await sleep(8000);
      }
      throw new Error("Timed out waiting for the jev-train issue. Check Actions if the run is still going.");
    }

    function download() {
      if (!state.draw) return;
      var payload = buildGrades(state.draw, state.marks, state.nonce || "");
      var blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
      var link = document.createElement("a");
      link.href = URL.createObjectURL(blob);
      link.download = "jev-train-" + (state.draw.stamp || "grades") + ".json";
      link.click();
      setTimeout(function () { URL.revokeObjectURL(link.href); }, 1000);
    }

    async function onNewDraw() {
      if (state.busy) return;
      state.busy = true;
      refreshSubmit();
      setErr("");
      var previous = state.draw && state.draw.stamp;
      try {
        await dispatch({ mode: "draw", stamp: "", grades_json: "", seed: "" });
        metaEl.textContent = "Draw dispatched. Waiting for draw.json on main.";
        await pollDraw(previous);
      } catch (err) {
        setErr(String(err && err.message ? err.message : err));
      } finally {
        state.busy = false;
        refreshSubmit();
      }
    }

    async function onSubmit() {
      if (state.busy || !marksReady(gradesList())) return;
      state.busy = true;
      refreshSubmit();
      setErr("");
      doneEl.hidden = true;
      state.nonce = (window.crypto && crypto.randomUUID) ? crypto.randomUUID() : String(Date.now());
      var payload = buildGrades(state.draw, state.marks, state.nonce);
      var packed = utf8ToB64(JSON.stringify(payload));
      try {
        if (packed.length > 55000) {
          throw new Error("Grades are too large to dispatch. Shorten notes or use Download grades JSON.");
        }
        await dispatch({
          mode: "grade",
          stamp: "",
          grades_json: packed,
          seed: ""
        });
        metaEl.textContent = "Grades dispatched. Waiting for the GitHub issue.";
        var url = await pollIssue(state.nonce);
        issueEl.href = url;
        issueEl.textContent = url;
        doneEl.hidden = false;
      } catch (err) {
        setErr(String(err && err.message ? err.message : err));
        download();
      } finally {
        state.busy = false;
        refreshSubmit();
      }
    }

    document.getElementById("newDraw").addEventListener("click", onNewDraw);
    document.getElementById("reload").addEventListener("click", function () {
      reload().catch(function (err) { setErr(String(err && err.message ? err.message : err)); });
    });
    document.getElementById("submit").addEventListener("click", onSubmit);
    document.getElementById("download").addEventListener("click", download);
    document.getElementById("clearToken").addEventListener("click", function () {
      document.getElementById("token").value = "";
    });
    reload().catch(function (err) { setErr(String(err && err.message ? err.message : err)); });
  }

  return {
    MIN_MARKS: MIN_MARKS,
    marksReady: marksReady,
    buildGrades: buildGrades,
    boot: boot
  };
});
