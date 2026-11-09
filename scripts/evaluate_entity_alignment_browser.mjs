// Execute the actual published-page comparison functions, without network I/O.
import fs from 'node:fs';
import vm from 'node:vm';
const input=JSON.parse(fs.readFileSync(0,'utf8'));
const html=fs.readFileSync(new URL('../dashboard/scanner-backtest/index.html',import.meta.url),'utf8');
const code=html.match(/<script>([\s\S]*?)<\/script>/)[1];
const elements=new Map();
const element=()=>({addEventListener(){},append(){},replaceChildren(){},setAttribute(){}});
const document={getElementById(id){if(!elements.has(id))elements.set(id,element());return elements.get(id);},createElement:element};
const context=vm.createContext({document,URLSearchParams,encodeURIComponent,setTimeout(){}});
vm.runInContext(code.replace('poll().then(() => loadArticle());',''),context);
context.payloads=input.fixtures;
vm.runInContext('globalThis.testIndexes=Object.fromEntries(Object.entries(payloads).map(([k,v])=>[k,prepareKeywordIndex(v)]));',context);
const results={};
for(const test of input.cases){
  context.testText=test.text;context.testVariant=test.variant || 'single';
  results[test.id]=vm.runInContext('compareKeywordText(testIndexes[testVariant],testText)',context);
}
process.stdout.write(JSON.stringify(results));
