import {chromium} from 'playwright';
import {mkdir,writeFile,readFile} from 'node:fs/promises';
import {gunzipSync} from 'node:zlib';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const BASE='https://sroyaltyy.github.io/fullscan/dashboard/scanner-backtest/compare/';
const RESULT='https://raw.githubusercontent.com/SRoyaltyy/fullscan/scanner-request-results/requests/';
const token=process.env.GH_TOKEN;
assert(token,'verification requires the temporary repository Actions token');
await mkdir('artifacts',{recursive:true});
const browser=await chromium.launch();
const page=await browser.newPage({viewport:{width:1440,height:1100}});
page.setDefaultTimeout(30000);
const report={page:BASE,checks:[],accuracy_certified:false};
let lastRequest,dispatchStatus;
page.on('request',request=>{
 if(request.url().endsWith('/actions/workflows/stock-parser-request.yml/dispatches')){
  lastRequest=JSON.parse(request.postData());dispatchStatus=undefined;
 }
});
page.on('response',response=>{
 if(response.url().endsWith('/actions/workflows/stock-parser-request.yml/dispatches'))dispatchStatus=response.status();
});
try{
 let config;
 for(let attempt=0;attempt<40;attempt++){
  try{
   const response=await fetch(BASE+'request-config.json?verify='+process.env.EXPECTED_UI_REVISION,{signal:AbortSignal.timeout(20000)});
   if(response.ok){config=await response.json();if(config.ui_revision===process.env.EXPECTED_UI_REVISION)break;}
  }catch{}
  await new Promise(resolve=>setTimeout(resolve,10000));
 }
 assert.equal(config?.ui_revision,process.env.EXPECTED_UI_REVISION,'expected deployed UI revision');
 await page.goto(BASE+'?verify='+process.env.EXPECTED_UI_REVISION,{waitUntil:'domcontentloaded'});
 await page.locator('#stock-summary').filter({hasText:'Connect GitHub'}).waitFor({timeout:90000});
 await page.getByLabel('GitHub token',{exact:true}).fill(token);
 await page.getByRole('button',{name:'Connect GitHub',exact:true}).click();
 await page.locator('#connection').filter({hasText:'GitHub connected'}).waitFor();
 assert.equal(await page.getByLabel('GitHub token',{exact:true}).inputValue(),'');
 report.checks.push('served revision and temporary GitHub connection');
 const catalog=await (await fetch(BASE+'catalog.json')).json();
 const description=Object.entries(catalog.catalog).find(([,row])=>row.coverage==='description_only')?.[0];
 assert(description,'catalog has description-only coverage controls');
 for(const ticker of ['LRCX',description]){
  await page.getByRole('combobox',{name:'Stock ticker'}).fill(ticker);
  await page.getByRole('combobox',{name:'Stock ticker'}).press('Enter');
  assert.equal(await page.locator('#rows tr').count(),0,'previous stock output clears immediately');
  const start=Date.now();
  while(Date.now()-start<65*60*1000){
   const error=await page.locator('#error').textContent();
   if(error)throw Error(ticker+': '+error);
   if((await page.locator('#stock-summary').textContent()).includes('fresh old/new execution'))break;
   await page.waitForTimeout(2000);
  }
  assert((await page.locator('#stock-summary').textContent()).includes(ticker+' ·'),'selected stock renders');
  assert((await page.locator('#stock-summary').textContent()).includes('fresh old/new execution'),'fresh execution completed');
  assert.equal(dispatchStatus,204,'real GitHub workflow dispatch succeeded');
  assert.equal(lastRequest.inputs.ticker,ticker);
  const receipt=await (await fetch(RESULT+lastRequest.inputs.request_id+'/receipt.json')).json();
  const raw=Buffer.from(await (await fetch(RESULT+lastRequest.inputs.request_id+'/comparison.json.gz')).arrayBuffer());
  assert.equal(createHash('sha256').update(raw).digest('hex'),receipt.sha256);
  const output=JSON.parse(gunzipSync(raw));
  assert.equal(output.request_id,lastRequest.inputs.request_id);
  assert.equal(output.ticker,ticker);
  assert.equal(output.old_sha,config.old_sha);
  assert.equal(output.new_sha,config.new_sha);
  assert.equal(output.old.input_sha256,output.input_sha256);
  assert.equal(output.new.input_sha256,output.input_sha256);
  assert.equal(output.catalog_sha256,config.catalog_sha256);
  if(ticker==='LRCX'){
   assert.equal(output.coverage_kind,'filing_backed');
   assert(output.source_receipt.filings>0);
   assert(await page.locator('#rows tr').count()>0);
   const buttons=page.locator('#rows button');
   for(let i=0;i<Math.min(20,await buttons.count());i++){
    await buttons.nth(i).click();
    if(await page.locator('#old-detail blockquote, #new-detail blockquote').count())break;
   }
   assert(await page.locator('#old-detail blockquote, #new-detail blockquote').count()>0,'supporting source evidence renders');
   await page.screenshot({path:'artifacts/live-LRCX.png',fullPage:true});
   await page.getByRole('searchbox',{name:'Search comparison terms'}).fill('__no_such_term_for_ui_control__');
   assert((await page.locator('#rows').textContent()).includes('No matching output terms'));
   await page.getByRole('searchbox',{name:'Search comparison terms'}).fill('');
   await page.getByRole('combobox',{name:'Change filter'}).selectOption('added');
   await page.getByRole('combobox',{name:'Change filter'}).selectOption('all');
  }else assert.equal(output.coverage_kind,'description_only');
  const downloadPromise=page.waitForEvent('download');
  await page.getByRole('button',{name:'Download comparison JSON'}).click();
  const download=await downloadPromise;
  const downloaded=JSON.parse(await readFile(await download.path(),'utf8'));
  assert.equal(downloaded.input_sha256,output.input_sha256);
  assert.equal(downloaded.ticker,ticker);
  await writeFile('artifacts/'+ticker+'-receipt.json',JSON.stringify(receipt,null,2));
  report.checks.push({ticker,request_id:output.request_id,run_id:output.run_id,
   input_sha256:output.input_sha256,coverage_kind:output.coverage_kind,timings:output.timings,
   browser_request_seconds:Math.round((Date.now()-start)/1000)});
  console.log(JSON.stringify(report.checks.at(-1)));
 }
 await page.getByRole('combobox',{name:'Stock ticker'}).fill('NOT_A_CATALOG_TICKER');
 await page.getByRole('combobox',{name:'Stock ticker'}).press('Enter');
 assert.equal(await page.locator('#rows tr').count(),0);
 assert((await page.locator('#error').textContent()).includes('Choose a catalog ticker'));
 report.checks.push('stock switching, exact evidence, search/no-match, change filter, JSON download, invalid ticker clears previous results');
 report.passed=true;
}catch(error){
 report.passed=false;report.error=error.message;
 await page.screenshot({path:'artifacts/live-failure.png',fullPage:true}).catch(()=>{});
 throw error;
}finally{
 await writeFile('artifacts/verification.json',JSON.stringify(report,null,2));
 await browser.close();
}
