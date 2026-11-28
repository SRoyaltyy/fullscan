import test from 'node:test';
import assert from 'node:assert/strict';
import {verifyReceipt,verifyBundle,api} from './request.mjs';
const req={request_id:'test-request-123456',ticker:'BLZE',old_sha:'a'.repeat(40),new_sha:'b'.repeat(40)};
const receipt={...req,input_sha256:'c'.repeat(64),sha256:'d'.repeat(64),run_id:'123',catalog_sha256:'e'.repeat(64),coverage_kind:'filing_backed'};
const bundle={...receipt,schema:'stock-parser-comparison-1',source_receipt:{filings:2},
 old:{ticker:req.ticker,source_sha:req.old_sha,input_sha256:receipt.input_sha256,source_receipt:{filings:2}},
 new:{ticker:req.ticker,source_sha:req.new_sha,input_sha256:receipt.input_sha256,source_receipt:{filings:2}}};
test('accepts one requested same-input pair',()=>{assert.equal(verifyReceipt(receipt,req,123),receipt);assert.equal(verifyBundle(bundle,receipt),bundle);});
test('rejects another request, stock, revision, run and corrupt hashes',()=>{
 for(const [field,value]of [['request_id','another'],['ticker','OTHER'],['old_sha',req.new_sha],['new_sha',req.old_sha],['run_id','124'],['sha256','corrupt'],['input_sha256','corrupt']])
  assert.throws(()=>verifyReceipt({...receipt,[field]:value},req,123));
});
test('rejects mixed-source parser outputs and envelope changes',()=>{
 for(const [role,field,value]of [['old','input_sha256','x'],['new','ticker','OTHER'],['new','source_sha',req.old_sha],['old','source_receipt',{}]]){
  const changed=structuredClone(bundle);changed[role][field]=value;assert.throws(()=>verifyBundle(changed,receipt));
 }
 for(const field of ['request_id','run_id','input_sha256','catalog_sha256'])assert.throws(()=>verifyBundle({...bundle,[field]:'wrong'},receipt));
});
test('never sends credentials to raw assets or unrelated APIs',async()=>{
 await assert.rejects(api('https://raw.githubusercontent.com/file','sensitive'));
 await assert.rejects(api('https://example.com/','sensitive'));
 await assert.rejects(api('/user','sensitive'));
});

test('dispatches requested pins and retrieves one checksum-verified workflow result',async()=>{
 const {requestComparison}=await import('./request.mjs');
 const {gzipSync}=await import('node:zlib');
 const {createHash}=await import('node:crypto');
 const saved=globalThis.fetch;let requested,receiptValue,resultBytes;const calls=[];
 const cfg={workflow:'stock-parser-request.yml',execution_ref:'main',catalog_revision:'f'.repeat(40),
            catalog_sha256:receipt.catalog_sha256,old_sha:req.old_sha,new_sha:req.new_sha};
 globalThis.fetch=async(url,options={})=>{
  const path=String(url);calls.push({url:path,options});
  if(path.endsWith('/dispatches')){
   requested=JSON.parse(options.body);
   const actual={...bundle,...requested.inputs,run_id:'123'};
   actual.old={...actual.old,ticker:requested.inputs.ticker};actual.new={...actual.new,ticker:requested.inputs.ticker};
   resultBytes=gzipSync(JSON.stringify(actual));
   receiptValue={...receipt,...requested.inputs,sha256:createHash('sha256').update(resultBytes).digest('hex')};
   return new Response(null,{status:204});
  }
  if(path.includes('/runs?'))return Response.json({workflow_runs:[{id:123,event:'workflow_dispatch',head_branch:'main',display_title:'Stock parser BLZE ['+requested.inputs.request_id+']',status:'in_progress',html_url:'https://github.com/SRoyaltyy/fullscan/actions/runs/123'}]});
  if(path.endsWith('/jobs'))return Response.json({jobs:[{steps:[{name:'Run old parser',status:'in_progress'}]}]});
  if(path.includes('receipt.json'))return Response.json(receiptValue);
  if(path.endsWith('comparison.json.gz'))return new Response(resultBytes);
  throw Error('Unexpected fetch '+path);
 };
 try{
  const states=[];const result=await requestComparison(cfg,'BLZE','test-only-token',{onProgress:p=>states.push(p.state)});
  assert.equal(result.ticker,'BLZE');assert.equal(result.old.input_sha256,result.new.input_sha256);
  assert.deepEqual(requested.inputs,{ticker:'BLZE',request_id:result.request_id,old_sha:cfg.old_sha,new_sha:cfg.new_sha,catalog_revision:cfg.catalog_revision});
  assert(states.includes('Run old parser'));
  for(const call of calls.filter(c=>c.url.startsWith('https://raw.githubusercontent.com/')))assert.equal(call.options.headers?.Authorization,undefined);
 }finally{globalThis.fetch=saved;}
});
