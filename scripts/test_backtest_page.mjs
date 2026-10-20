// DOM-state smoke checks, not a substitute for a visual browser review.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {gzipSync} from 'node:zlib';
import {webcrypto} from 'node:crypto';
const html = fs.readFileSync(new URL('../dashboard/scanner-backtest/index.html', import.meta.url), 'utf8');
const code = html.match(/<script>([\s\S]*?)<\/script>/)[1];
new vm.Script(code);
const elements = new Map();
function element() {
  return {textContent: '', hidden: false, disabled: false, children: [],
    append(...children) { this.children.push(...children); },
    addEventListener() {}, scrollIntoView() {}, remove() {}, setAttribute() {}, replaceChildren(...children) {this.children = children;}};
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
const packed = gzipSync(JSON.stringify({facts: [{predicate: 'buys_from', object: 'Example Supplier'}]}));
context.crypto = webcrypto;
context.Blob = Blob;
context.Response = Response;
context.DecompressionStream = DecompressionStream;
context.fetch = async () => new Response(packed);
context.profileHash = Buffer.from(await webcrypto.subtle.digest('SHA-256', packed)).toString('hex');
vm.runInContext('staticMeta = {snapshot:{snapshot_id:"test"}}', context);
const decoded = await vm.runInContext('loadPackedProfile("scoring-profiles/aa.json.gz", profileHash)', context);
assert.equal(decoded.facts[0].object, 'Example Supplier');
await assert.rejects(vm.runInContext('loadPackedProfile("../outside.json.gz", profileHash)', context), /Invalid profile asset path/);
await assert.rejects(vm.runInContext('loadPackedProfile("scoring-profiles/aa.json.gz", "wrong")', context), /does not match/);
console.log('Compressed profile loading verifies hashes and rejects unsafe paths and mismatched snapshots.');

