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
