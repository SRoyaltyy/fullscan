/* Five-round status. This file never handles API keys or workflow tokens. */
(function () {
  var panel = document.getElementById('acceptance');
  if (!panel) return;
  function percent(value) { return (100 * value).toFixed(1) + '%'; }
  fetch('acceptance.json?t=' + Date.now(), {cache: 'no-store'}).then(function (response) {
    if (!response.ok) throw new Error('No five-round result recorded yet.');
    return response.json();
  }).then(function (report) {
    var status = document.createElement('p');
    status.textContent = 'Consecutive passes: ' + report.streak + ' / 5. ' +
      (report.accepted ? 'Acceptance target met.' : 'Acceptance target not met.') +
      ' Evidence: ' + (report.evidence_scope || 'not recorded');
    panel.replaceChildren(status);
    var models = document.createElement('p');
    models.className = 'meta';
    models.textContent = 'JEV: ' + report.jev_model + ' · Frontier: ' + report.teacher_model +
      ' · Both class recalls must be strictly above 80%; every round has 100 labels.';
    panel.appendChild(models);
    var table = document.createElement('table');
    var headings = ['Round', 'Useful kept', 'Trash discarded', 'Keep precision', 'Overall', 'Fresh', 'Result', 'Streak'];
    var header = document.createElement('tr');
    headings.forEach(function (name) { var cell = document.createElement('th'); cell.textContent = name; header.appendChild(cell); });
    var head = document.createElement('thead'); head.appendChild(header); table.appendChild(head);
    var body = document.createElement('tbody');
    report.rounds.forEach(function (round) {
      var row = document.createElement('tr');
      [round.round, percent(round.useful_recall), percent(round.trash_recall), percent(round.keep_precision),
       percent(round.overall_accuracy), round.fresh ? 'Yes' : 'No', round.pass ? 'PASS' : 'FAIL', round.streak].forEach(function (value) {
        var cell = document.createElement('td'); cell.textContent = String(value); row.appendChild(cell);
      });
      body.appendChild(row);
    });
    table.appendChild(body); panel.appendChild(table);
  }).catch(function (error) { panel.textContent = error.message; });
})();
