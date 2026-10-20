// DOM-state smoke checks, not a substitute for a visual browser review.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const html = fs.readFileSync(new URL('../dashboard/scanner-backtest/index.html', import.meta.url), 'utf8');
const code = html.match(/<script>([\s\S]*?)<\/script>/)[1];
new vm.Script(code);
const elements = new Map();
function element() {
  return {textContent: '', hidden: false, disabled: false, children: [],
    append(...children) { this.children.push(...children); },
    addEventListener() {}, scrollIntoView() {}};
}
const document = {
  getElementById(id) { if (!elements.has(id)) elements.set(id, element()); return elements.get(id); },
  createElement: element,
};
const context = vm.createContext({document, setTimeout() {}, URLSearchParams, encodeURIComponent});
// Startup performs network I/O; state tests call handlers directly instead.
vm.runInContext(code.replace('poll().then(() => loadArticle());', ''), context);
vm.runInContext('renderSnapshot(null)', context);
assert.match(elements.get('snapshot').textContent, /Historical cache/);
vm.runInContext('renderSnapshot({snapshot_id:"0123456789abcdef",compiler_version:"stock-evidence-2.7",input_mode:"headline",filing_coverage:{covered_tickers:["META"],catalog_tickers:5961,as_of:"2026-10-19"}})', context);
assert.match(elements.get('snapshot').textContent, /input=headline/);
assert.match(elements.get('snapshot').textContent, /1\/5961/);
vm.runInContext('renderArticle({id:"test",title:"headline",text:"headline",input_mode:"headline"})', context);
assert.match(elements.get('textbadge').textContent, /primary headline only/);
console.log('Page JavaScript, historical-cache warning, source coverage, and headline state passed.');
