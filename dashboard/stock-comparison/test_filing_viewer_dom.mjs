import test from 'node:test';import assert from 'node:assert/strict';import {createRequire} from 'node:module';import {createHash} from 'node:crypto';
import {mountViewer} from './filing-viewer.mjs';
const {JSDOM}=createRequire(import.meta.url)('../../.viewer-test/node_modules/jsdom');
test('Filing reader preserves text, filters, version switches and source changes',async()=>{
 const dom=new JSDOM('<section id="viewer" hidden></section>');globalThis.document=dom.window.document;
 try{
 const hash=t=>createHash('sha256').update(t).digest('hex');
 const full='A🙂 Bank of America and Alice. <script>literal</script>\n'+'Unhighlighted context. '.repeat(800)+'Unique tailword.';
 const mk=(id,text)=>({id,old_source_id:'old-'+id,new_source_id:'new-'+id,text,characters:Array.from(text).length,sha256:hash(text),kind:'business',document:'filing.htm',section:'Business'});
 const source=mk('one',full),second=mk('two','Second document without recognized terms.');
 const bundle={source_texts:[source,second],new:{raw_terms:[{source_id:'new-one',start:3,end:18,text:'Bank of America',types:['ORG']},{source_id:'new-one',start:23,end:28,text:'Alice',types:['PERSON']}]},old:{keywords:[{keyword:'Bank of America',scanner:{eligible:true},evidence_ids:['e']}],keyword_evidence:{e:{witnesses:[{source_id:'old-one',argument:{start:3,end:18,text:'Bank of America'}}]}}}};
 const host=document.getElementById('viewer'),viewer=mountViewer(host),get=r=>host.querySelector('[data-role="'+r+'"]');
 await viewer.load(bundle);
 const page=Array.from(full).slice(0,12000).join('');
 assert.equal(get('text').textContent,page);assert.equal(get('text').querySelector('script'),null);
 assert.equal(get('text').querySelectorAll('mark').length,2);
 get('text').querySelector('mark').click();assert.match(get('detail').textContent,/Bank of America/);
 get('version').value='old';get('version').dispatchEvent(new dom.window.Event('change'));
 assert.equal(get('text').textContent,page);assert.equal(get('text').querySelectorAll('mark').length,1);
 get('version').value='new';get('version').dispatchEvent(new dom.window.Event('change'));
 for(const checkbox of get('legend').querySelectorAll('input')){checkbox.checked=false;checkbox.dispatchEvent(new dom.window.Event('change'));}
 assert.equal(get('text').querySelectorAll('mark').length,0);assert.equal(get('text').textContent,page);
 get('search').value='Unique tailword';get('find').click();assert.match(get('text').textContent,/Unique tailword/);
 get('source').value='1';get('source').dispatchEvent(new dom.window.Event('change'));assert.equal(get('text').textContent,second.text);
 assert.equal(get('detail').textContent,'Click a highlight to inspect its exact source span.');
 await viewer.load({source_texts:[second],new:{raw_terms:[]},old:{keywords:[]}});
 assert.equal(get('source').options.length,1);
 const pending=viewer.load(bundle);viewer.clear();await pending;
 assert(host.hidden);assert.equal(get('text').textContent,'');
 await viewer.load({});assert.match(get('status').textContent,/no full filing text/);
 }finally{delete globalThis.document;dom.window.close();}
});
