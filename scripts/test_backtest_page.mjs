// DOM-state smoke checks, not a substitute for a visual browser review.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {gzipSync} from 'node:zlib';
import {webcrypto} from 'node:crypto';
import {spawnSync} from 'node:child_process';
const html = fs.readFileSync(new URL('../dashboard/scanner-backtest/index.html', import.meta.url), 'utf8');
const code = html.match(/<script>([\s\S]*?)<\/script>/)[1];
new vm.Script(code);
const elements = new Map();
function element() {
  return {textContent: '', hidden: false, disabled: false, children: [],
    append(...children) { this.children.push(...children); },
    listeners: {}, addEventListener(name, fn) {this.listeners[name] = fn;}, scrollIntoView() {}, remove() {}, setAttribute() {}, replaceChildren(...children) {this.children = children;}};
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
context.grammarArticle={id:'grammar',text:'DraftKings faces rules',input_mode:'headline',article_parse:{
  input_sha256:'a'.repeat(64),parser:{spacy:'3.8.11',model:'en_core_web_sm',model_version:'3.8.0'},
  mentions:[{start:0,end:10,label:'ORG'}],noun_phrases:[{start:0,end:10,head:0},{start:17,end:22,head:2}],
  syntax:[{start:0,end:10,lemma:'DraftKings',pos:'PROPN',dependency:'nsubj',head:1},
          {start:11,end:16,lemma:'face',pos:'VERB',dependency:'ROOT',head:1},
          {start:17,end:22,lemma:'rule',pos:'NOUN',dependency:'dobj',head:1}]}};
vm.runInContext('current=grammarArticle;renderSpacy(current.id)',context);
assert.equal(elements.get('spacy').hidden,false);
assert.match(elements.get('spacy-cluster').textContent,/en_core_web_sm/);
assert.ok(elements.get('spacy-body').children.some(e=>e.textContent==='DraftKings → face → rules'));
vm.runInContext('staticMeta={snapshot:{filing_coverage:{scope:"deep_filing_relationships",covered_tickers:["BLZE"]}}};selectedTicker="BLZE";lastPairResult={passes:[],keyword_comparison:{matches:[],unresolved_matches:[],rejected_matches:[]}};renderSelectedPair()',context);
assert.match(elements.get('stock-article-pair').textContent,/SEC-backed stock/);
assert.match(elements.get('stock-article-pair').textContent,/No aligned keyword evidence/);
vm.runInContext('selectedTicker="AAPL";renderSelectedPair()',context);
assert.match(elements.get('stock-article-pair').textContent,/No complete SEC graph/);
console.log('Cached article grammar and stock/article pair coverage are visible without a stock pass.');
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
context.paragraphBox = element();
context.testGraph = {facts: [], sources: [{id:'s', form:'10-K', filing_date:'2026-01-01'}],
  coverage:{paragraph_outcomes:{unresolved:101}}, paragraphs:Array.from({length:101}, (_,i) => ({
    id:String(i), source_id:'s', start:i, end:i+1, text:`Paragraph ${i} <script>unsafe</script>`,
    labels:['unresolved_topic'], extraction_status:'unresolved', review_required:true,
    importance:{priority:'review', reasons:['No topic rule matched']}, relationship_ids:[], signals:[], classification_evidence:[]
  }))};
vm.runInContext('renderParagraphs(paragraphBox, testGraph)', context);
const box = context.paragraphBox;
assert.match(box.children[0].textContent, /101/);
const paragraphList = box.children.at(-1);
assert.match(paragraphList.children[0].textContent, /101 of 101/);
const more = paragraphList.children.find(e => e.textContent.startsWith('Show more'));
assert.ok(more, 'All paragraphs must remain reachable through pagination');
more.listeners.click();
const detailRows = paragraphList.children.filter(e => e.children?.[0]?.textContent.startsWith('[review]'));
assert.equal(detailRows.length, 101);
assert.equal(detailRows.at(-1).children[3].textContent, 'Paragraph 100 <script>unsafe</script>');
box.children[2].value = 'Paragraph 100';
box.children[2].listeners.input();
assert.match(paragraphList.children[0].textContent, /1 of 101/);
box.children[2].value = '';
box.children[5].value = 'high';
box.children[5].listeners.change();
assert.match(paragraphList.children[0].textContent, /0 of 101/);
console.log('Every paragraph is reachable, searchable, filterable, and rendered as safe full text.');
context.termBox=element();
context.termGraph={...context.testGraph,paragraphs:[{...context.testGraph.paragraphs[0],term_ids:Array.from({length:101},(_,i)=>String(i))}],
  term_inventory:{terms:Array.from({length:101},(_,i)=>({id:String(i),text:`term ${i} <script>`,start:i,end:i+1,
    entity_types:[],kinds:['noun_token'],relationships:[],context:{noun_phrase:{text:'office space'}},
    capacity:{dependency:'dobj',governor:{text:'lease'}}}))}};
vm.runInContext('renderParagraphs(termBox,termGraph)',context);
const termParagraph=context.termBox.children.at(-1).children.find(e=>e.children?.[0]?.textContent.startsWith('[review]'));
const termInventory=termParagraph.children.find(e=>e.children?.[0]?.textContent.startsWith('Entities and terminology'));
assert.ok(termInventory);
termInventory.children.find(e=>e.textContent.startsWith('Show more')).listeners.click();
assert.equal(termInventory.children.filter(e=>e.textContent.startsWith('term ')).length,101);
assert.ok(termInventory.children.some(e=>e.textContent.startsWith('term 100 <script>')));
console.log('All term occurrences remain reachable with their phrase context and unresolved relationships.');

// Same term, separate paragraphs/senses; an emoji before the term exercises
// Python code-point offsets rather than JavaScript UTF-16 indexing.
context.reviewBox=element();
context.reviewGraph={sources:[{id:'s',form:'10-K',filing_date:'2026-01-01',accession:'filing-1'}],
  paragraphs:[{id:'office',source_id:'s',start:0,end:16,text:'🛰 We lease space'},
    {id:'orbit',source_id:'s',start:17,end:29,text:'Space flight'}],
  facts:[{id:'f',subject:'BLZE',predicate:'uses_input',object:'space',status:'asserted',rule:'test:lease',
    evidence:[{source_id:'s',start:0,end:16,quote:'🛰 We lease space',trigger:{start:5,end:10,text:'lease'},argument:{start:11,end:16,text:'space'}}]}],
  term_inventory:{terms:[{id:'t1',source_id:'s',text:'space',start:11,end:16,paragraph_ids:['office'],
    kinds:['noun_token'],entity_types:[],context:{noun_phrase:{text:'space'}},capacity:{dependency:'dobj',governor:{text:'lease'}},
    relationships:[{relationship_id:'f',subject:'BLZE',predicate:'uses_input',object:'space',assertion:'asserted',attachment:'within_extracted_argument'}]},
    {id:'t2',source_id:'s',text:'Space',start:17,end:22,paragraph_ids:['orbit'],kinds:['noun_token'],entity_types:[],
      context:{noun_phrase:{text:'Space flight'}},capacity:{dependency:'compound',governor:{text:'flight'}},relationships:[]}]}};
context.inspectDecision=vm.runInContext('renderTermReview(reviewBox,reviewGraph)',context);
const reviewRoot=context.reviewBox.children[0], reviewList=reviewRoot.children.at(-1);
assert.match(reviewList.children[0].textContent,/2 of 2/);
const firstTerm=reviewList.children[1], secondTerm=reviewList.children[2];
assert.match(firstTerm.children[2].textContent,/test:lease/);
assert.match(secondTerm.children[0].textContent,/relationship unresolved/);
const sourceParagraph=firstTerm.children.at(-1);
assert.match(sourceParagraph.children[0].textContent,/Paragraph office/);
assert.equal(sourceParagraph.children[1].children[1].textContent,'space');
assert.equal(sourceParagraph.children[1].children.map(c=>c.textContent).join(''),'🛰 We lease space');
context.inspectDecision({stock:'BLZE',relationship:'input',keyword:'space',assertion:'asserted',term_occurrence_ids:['t1'],evidence_ids:['f']});
assert.match(reviewList.children[0].textContent,/1 of 2/);
assert.ok(reviewList.children.some(c=>c.textContent.includes('Trigger: lease')));
context.inspectDecision({stock:'BLZE',relationship:'issuer',keyword:'BLZE',assertion:'identity',term_occurrence_ids:[],evidence_ids:[]});
assert.ok(reviewList.children.some(c=>c.textContent.includes('No complete SEC paragraph witness')));
reviewRoot.children[3].listeners.click();
assert.match(reviewList.children[0].textContent,/2 of 2/);
reviewRoot.children[2].value='space';reviewRoot.children[2].listeners.input();
assert.match(reviewList.children[0].textContent,/2 of 2/);
console.log('Keyword decisions open exact paragraphs, preserve Unicode spans, expose rule evidence, and keep repeated unresolved occurrences separate.');

// Both paths must match the same normalized phrase, direction, status and exact
// article offsets. Python exports relevant dictionary forms to the browser.
const parity = spawnSync('python',['-I','-S','-c',`
import json,sys
sys.path.insert(0,sys.argv[1])
from src.test_stock_keywords import graph,profile
from src.stock_keywords import compile_keywords,KeywordIndex
from src.test_entity_resolution import fixture,CASES
index=KeywordIndex({'BLZE':profile(compile_keywords(graph(),as_of='2026-04-01'))})
texts=['Western Digital faces hard-drive disruption','Stripe disruption; Backblaze shares fall','Western. Digital shares rise','unrelated article','HARD DRIVES and Western Digital','📉 Western Digital hard drives']
collision=KeywordIndex(fixture())
print(json.dumps({'payload':index.browser_payload(),'cases':[{'text':t,'result':index.compare(t)} for t in texts],
 'collisions':{'payload':collision.browser_payload(),'cases':[{'text':t,'result':collision.compare(t)} for t,_,_ in CASES]}}))
`,new URL('..',import.meta.url).pathname],{encoding:'utf8',maxBuffer:10_000_000});
assert.equal(parity.status,0,parity.stderr);
const fixture = JSON.parse(parity.stdout);
context.parityPayload = fixture.payload;
vm.runInContext('keywordSearchIndex = prepareKeywordIndex(parityPayload)',context);
const signature = result => result.matches.map(r => [r.id,r.stock,r.relationship,r.assertion,r.current_as_of_cutoff,
  r.usable_for_scoring,r.article_span.start,r.article_span.end,r.article_span.text]).sort((a,b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
for (const test of fixture.cases) {
  context.parityText = test.text;
  const actual = vm.runInContext('compareKeywordText(keywordSearchIndex,parityText)',context);
  assert.equal(JSON.stringify(signature(actual)),JSON.stringify(signature(test.result)),test.text);
  assert.equal(actual.stock_passes_changed,false);
}
context.collisionPayload=fixture.collisions.payload;
vm.runInContext('keywordSearchIndex=prepareKeywordIndex(collisionPayload)',context);
const identitySignature=result=>['matches','unresolved_matches','rejected_matches'].map(key=>[
  key,(result[key] || []).map(r=>[r.id,r.alignment.status,r.alignment.stock_entity_id,r.alignment.article_entity_id,
    r.alignment.source_type,r.alignment.context_score,r.alignment.reasons,r.article_span.text]).sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b)))
]);
for (const test of fixture.collisions.cases) {
  context.parityText=test.text;
  const actual=vm.runInContext('compareKeywordText(keywordSearchIndex,parityText)',context);
  assert.equal(JSON.stringify(identitySignature(actual)),JSON.stringify(identitySignature(test.result)),test.text);
}
console.log('Person/company, same-surname, employer scope, and contextual concept sense decisions agree in Python and browser.');
context.keywordBox = element();
context.keywordRows = fixture.payload.phrases.filter(p => p.row.relationship !== 'issuer').map(p => p.row);
vm.runInContext('renderKeywordRecords(keywordBox,keywordRows)',context);
assert.match(context.keywordBox.children[1].children[0].textContent,/keyword records/);
vm.runInContext('renderMatches({decision:"no_pass",pass_count:0,passes:[],keyword_comparison:{matches:keywordRows,broad_matches:[]}})',context);
assert.match(elements.get('article-keywords').children[0].textContent,/relationship/);
console.log('Python/browser keyword matching parity, exact offsets, direction labels, and no-pass visibility passed.');

const assets = new Map(), buckets = {};
for (const entry of fixture.payload.phrases) {
  const key = (entry.phrase[0].charCodeAt(0)%16).toString(16);
  (buckets[key] ||= []).push(entry);
}
const manifest = {...fixture.payload,buckets:{}}; delete manifest.phrases;
for (const [key,rows] of Object.entries(buckets)) {
  const bytes = gzipSync(JSON.stringify(rows)), path = `keyword-index/${key}.json.gz`;
  const sha256 = Buffer.from(await webcrypto.subtle.digest('SHA-256',bytes)).toString('hex');
  assets.set('./'+path,bytes); manifest.buckets[key] = {path,sha256};
}
const manifestBytes = gzipSync(JSON.stringify(manifest));
assets.set('./keyword-index.json.gz',manifestBytes);
context.manifestHash = Buffer.from(await webcrypto.subtle.digest('SHA-256',manifestBytes)).toString('hex');
const requested = [];
context.fetch = async url => {requested.push(url.split('?')[0]); const bytes=assets.get(url.split('?')[0]); return bytes ? new Response(bytes) : new Response('missing',{status:404});};
vm.runInContext('mode="static"; staticMeta={snapshot:{snapshot_id:"test"},keyword_index:{path:"keyword-index.json.gz",sha256:manifestHash}}; keywordManifest=null;',context);
document.getElementById('keyword-text').value = 'Western Digital';
await vm.runInContext('compareCustomKeywords()',context);
assert.ok(requested.includes('./keyword-index/7.json.gz'));
assert.ok(!requested.includes('./keyword-index/8.json.gz'),'Load only relevant keyword buckets');
document.getElementById('keyword-text').value = 'hard-drives';
await vm.runInContext('compareCustomKeywords()',context);
assert.ok(requested.includes('./keyword-index/8.json.gz'));
assert.equal(elements.get('compare-keywords').disabled,false);
console.log('Custom article comparisons load and verify keyword buckets as needed.');


