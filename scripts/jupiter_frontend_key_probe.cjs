/* Compare the publishable frontend config in two fresh, wallet-free contexts. */
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const {proxySettings}=require('./jupiter_proxy.cjs');
const {chromium}=require(process.env.PLAYWRIGHT_MODULE||'playwright');
const root=path.resolve(process.env.JUPITER_DATA||'data/jupiter');
const output=path.resolve(process.env.JUPITER_PROOF_OUTPUT||root);
fs.mkdirSync(output,{recursive:true});
const manifest=JSON.parse(fs.readFileSync(path.join(root,'js-manifest.json')));
const runtime=manifest.find(r=>r.file.includes('vendor-tanstack-runtime'));
const source=fs.readFileSync(path.join(root,runtime.file),'utf8');
const key=source.match(/apiKey\s*:\s*[\w$]+\.swapApiKey\s*\?\?\s*['"`]([\w.-]+)['"`]/)?.[1];
if(!key)throw Error('Missing pinned publishable frontend config');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const redact=u=>{const x=new URL(u);for(const k of [...x.searchParams.keys()])if(/key|token|secret/i.test(k))x.searchParams.set(k,'[REDACTED]');return x.href;};
async function main(){
 const browser=await chromium.launch({headless:true,channel:'chrome',proxy:proxySettings(),args:['--disable-quic','--force-webrtc-ip-handling-policy=disable_non_proxied_udp']}),results=[];
 try {
  for(let index=1;index<=2;index++) {
   const context=await browser.newContext({locale:'en-US'}),page=await context.newPage();
   const jobs=[];let servedRuntimeHash=null;
   context.on('request',r=>fs.appendFileSync(path.join(output,'key-probe-requests.jsonl'),JSON.stringify({context:index,method:r.method(),url:redact(r.url()),resourceType:r.resourceType()})+'\n'));
   context.on('response',r=>{if(r.url()===runtime.url)jobs.push(r.body().then(b=>{servedRuntimeHash=sha(b);}));});
   await page.goto('https://jup.ag/swap?buy=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v&sell=So11111111111111111111111111111111111111112&ultra_compute=swap-api',{waitUntil:'domcontentloaded',timeout:45000});
   await page.getByRole('textbox',{name:'Sell amount'}).waitFor({timeout:30000});
   const requestPromise=page.waitForRequest(r=>r.url().includes('/swap/v2a/order?'),{timeout:30000});
   await page.waitForTimeout(3000);
   await page.getByRole('textbox',{name:'Sell amount'}).fill('0.01');
   const request=await requestPromise;const url=new URL(request.url());
   const response=await request.response();await Promise.allSettled(jobs);
   const apiResource=await context.request.get(runtime.url,{timeout:20000});
   const apiBody=await apiResource.body();
   results.push({context:index,freshCookies:true,walletConnected:false,endpoint:url.origin+url.pathname,
    keyPresent:url.searchParams.has('apiKey'),keyMatchesPinnedFallback:url.searchParams.get('apiKey')===key,
    runtimeMatchesArchive:servedRuntimeHash===runtime.sha256,clientPlatform:url.searchParams.get('clientPlatform'),
    amount:url.searchParams.get('amount'),status:response?.status(),url:redact(request.url()),
    contextRequestStatus:apiResource.status(),contextRequestMatchesArchive:sha(apiBody)===runtime.sha256});
   await context.close();
  }
 }finally{await browser.close();}
 const report={observedAt:new Date().toISOString(),proxied:true,sourceFile:runtime.file,sourceSha256:runtime.sha256,contexts:results,
  conclusion:results.every(r=>r.keyMatchesPinnedFallback&&r.runtimeMatchesArchive)?'Two independent browsers used exactly the same key embedded in the pinned public production bundle. No per-browser key generation observed.':'Evidence mismatch: inspect current deployment.'};
 fs.writeFileSync(path.join(output,'frontend-key-proof.json'),JSON.stringify(report,null,2));console.log(JSON.stringify(report));
}
main().catch(e=>{console.error('Jupiter key probe stopped:',e.name,'; check proxy configuration and browser status');process.exitCode=1;});
