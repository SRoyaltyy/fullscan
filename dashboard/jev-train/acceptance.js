/* Ten-round status. This file never handles API keys or workflow tokens. */
(function () {
  var panel = document.getElementById('acceptance');
  if (!panel) return;
  function percent(value) { return (100 * value).toFixed(1) + '%'; }
  fetch('acceptance.json?t=' + Date.now(), {cache: 'no-store'}).then(function (response) {
    if (!response.ok) throw new Error('No ten-round result recorded yet.');
    return response.json();
  }).then(function (report) {
    var status = document.createElement('p');
    status.textContent = 'Consecutive passes: ' + report.streak + ' / ' + (report.required_rounds || 10) + '. ' +
      (report.accepted ? 'Acceptance target met.' : 'Acceptance target not met.') +
      ' Evidence: ' + (report.evidence_scope || 'not recorded');
    panel.replaceChildren(status);
    var models = document.createElement('p');
    models.className = 'meta';
    models.textContent = 'JEV: ' + report.jev_model + ' · Frontier: ' + report.teacher_model +
      ' · Both class recalls must be strictly above 80%; every round has 100 labels.';
    panel.appendChild(models);
    var table = document.createElement('table');
    var headings = ['Round', 'Role', 'Useful kept', 'Trash discarded', 'Keep precision', 'Overall', 'Fresh', 'Result', 'Streak'];
    var header = document.createElement('tr');
    headings.forEach(function (name) { var cell = document.createElement('th'); cell.textContent = name; header.appendChild(cell); });
    var head = document.createElement('thead'); head.appendChild(header); table.appendChild(head);
    var body = document.createElement('tbody');
    report.rounds.forEach(function (round) {
      var row = document.createElement('tr');
      [round.round, round.dataset_role || 'acceptance', percent(round.useful_recall), percent(round.trash_recall), percent(round.keep_precision),
       percent(round.overall_accuracy), round.fresh ? 'Yes' : 'No', round.pass ? 'PASS' : 'FAIL', round.streak].forEach(function (value) {
        var cell = document.createElement('td'); cell.textContent = String(value); row.appendChild(cell);
      });
      body.appendChild(row);
    });
    table.appendChild(body); panel.appendChild(table);
  }).catch(function (error) { panel.textContent = error.message; });
})();

(function () {
 var panel=document.getElementById('acceptance'); if(!panel)return;
 var box=document.createElement('section');panel.after(box);
 var h=document.createElement('h3');h.textContent='Exact article grading records';box.appendChild(h);
 var note=document.createElement('p');note.textContent='Every historical and Lane evaluation is included, including development and failed rounds. Old-rubric passes do not count toward the Lane target. For blind regrading, download the evidence-only JSON and freeze your labels before viewing comparisons. Historical regrades do not count as fresh acceptance.';box.appendChild(note);
 [['Blind evidence JSON','blind-regrade.json'],['Full comparison CSV','grade-records.csv'],['Full comparison JSON','grade-records.json'],['All records as a Markdown table','grade-records.md']].forEach(function(x){var a=document.createElement('a');a.textContent=x[0];a.href=x[1];box.appendChild(a);box.appendChild(document.createTextNode(' · '));});
 var select=document.createElement('select');select.setAttribute('aria-label','Comparison round');box.appendChild(document.createElement('br'));box.appendChild(select);
 var table=document.createElement('table');box.appendChild(table);
 fetch('grade-records.json?t='+Date.now(),{cache:'no-store'}).then(function(r){if(!r.ok)throw Error('Records unavailable');return r.json();}).then(function(d){
 Array.from(new Set(d.records.map(function(r){return r.round;}))).forEach(function(n){var o=document.createElement('option');o.value=n;o.textContent=String(n).replace('historical-','Old rubric round ').replace('lane-','Lane batch ');select.appendChild(o);});
 function render(){table.replaceChildren();var tr=document.createElement('tr');['ID / role','Exact headline / source / date','Frontier','JEV','Outcome','JEV reason'].forEach(function(s){var th=document.createElement('th');th.textContent=s;tr.appendChild(th);});var head=document.createElement('thead');head.appendChild(tr);table.appendChild(head);var body=document.createElement('tbody');d.records.filter(function(r){return String(r.round)===select.value;}).forEach(function(r){var tr=document.createElement('tr');[r.id+' / '+(r.dataset_role||'acceptance'),r.title+' — '+r.source+' — '+r.published_at,r.frontier_grade,r.jev_grade,r.match?'Match':(r.frontier_grade==='keep'?'Miss: useful discarded':'Miss: trash kept'),r.jev_reason].forEach(function(s,i){var td=document.createElement('td');if(i===1&&r.url){var a=document.createElement('a');a.href=r.url;a.textContent=s;a.target='_blank';a.rel='noopener';td.appendChild(a);}else td.textContent=s;tr.appendChild(td);});body.appendChild(tr);});table.appendChild(body);}
 var latest=d.records.filter(function(r){return r.rubric_group==='lane'&&r.dataset_role==='acceptance';}).slice(-1)[0];if(latest)select.value=String(latest.round);select.onchange=render;render();
 }).catch(function(e){var p=document.createElement('p');p.textContent=e.message;box.appendChild(p);});
})();
