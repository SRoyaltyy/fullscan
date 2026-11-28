import test from 'node:test';import assert from 'node:assert/strict';
import {segments,annotations,verifySources,category} from './filing-viewer.mjs';
test('Unicode positions, overlaps and pagination preserve every source character',()=>{
 const text='A🙂 Bank of America. More unhighlighted text.';
 const rows=[{start:3,end:18,text:'Bank of America',category:'organization'},{start:11,end:18,text:'America',category:'place'}];
 const all=segments(text,rows);assert.equal(all.map(s=>s.text).join(''),text);assert(all.some(s=>s.annotations.length===2));
 assert.equal(segments(text,rows,0,12).map(s=>s.text).join('')+segments(text,rows,12).map(s=>s.text).join(''),text);
 assert.equal(segments(text,rows,0,Array.from(text).length,new Set()).map(s=>s.text).join(''),text);
});
test('Invalid spans are rejected and old anchors stay in their source',()=>{
 const s={id:'s',text:'A🙂 Bank of America.',old_source_id:'o'};
 const result=annotations({new:{raw_terms:[{source_id:'s',start:3,end:18,text:'Bank of America',types:['ORG']},{source_id:'s',start:3,end:7,text:'WRONG'}]}},s,'new');
 assert.equal(result.rows.length,1);assert.equal(result.invalid.length,1);
 const old=annotations({old:{keywords:[{keyword:'Bank',scanner:{eligible:true},evidence_ids:['e']}],keyword_evidence:{e:{witnesses:[{source_id:'o',argument:{start:3,end:7,text:'Bank'}}]}}}},s,'old');
 assert.equal(old.rows.length,1);assert.equal(old.rows[0].category,'unclassified');
});
test('Source checksums and codepoint lengths must agree',async()=>{
 const text='A🙂 <script>literal</script>';const bytes=new TextEncoder().encode(text);
 const sha=[...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(x=>x.toString(16).padStart(2,'0')).join('');
 await verifySources([{text,sha256:sha,characters:Array.from(text).length}]);
 await assert.rejects(()=>verifySources([{text:text+'x',sha256:sha,characters:Array.from(text).length}]));
 assert.equal(category(['LAW']),'law');assert.equal(category([]),'unclassified');
});
