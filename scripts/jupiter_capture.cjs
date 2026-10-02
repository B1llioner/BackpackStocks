/* Isolated public Jupiter capture. No funded wallet and no transaction signing. */
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const readline = require('node:readline');
const { proxySettings } = require('./jupiter_proxy.cjs');
const { createRequire } = require('node:module');
const pw = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const bs58Module = createRequire(path.resolve('local_clmm/package.json'))('bs58');
const bs58 = bs58Module.default || bs58Module;
const out = path.resolve(process.env.JUPITER_DATA || 'data/jupiter');
for (const dir of ['js', 'responses', 'snapshots']) fs.mkdirSync(path.join(out, dir), {recursive:true});
const secret = /^(authorization|cookie|set-cookie|x-api-key|apiKey|swapApiKey|access[_-]?token|refresh[_-]?token|authToken|sessionToken|idToken|csrfToken|turnstileToken|token|password|privateKey|secret|seed|mnemonic|jwt|signature|signedTransaction|x-turnstile-token|proxy-authorization|proxy-authenticate)$/i;
function clean(value) {
  if (Array.isArray(value)) return value.map(clean);
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([k,v])=>[k,secret.test(k)?'[REDACTED]':clean(v)]));
  return value;
}
function safeUrl(value) {
  try { const u = new URL(value); for(const k of [...u.searchParams.keys()]) if(secret.test(k)||/api.?key|token|secret/i.test(k)) u.searchParams.set(k,'[REDACTED]'); return u.href; } catch { return value; }
}
function body(value) { if(!value) return value; try{return clean(JSON.parse(value));}catch{return '[non-JSON body omitted]';} }
const append = (name, value) => fs.appendFileSync(path.join(out,name), JSON.stringify(clean(value))+'\n');
const save = (name, value) => fs.writeFileSync(path.join(out,name), JSON.stringify(clean(value),null,2));
const sha = value => crypto.createHash('sha256').update(value).digest('hex');
const requests = [], manifest = [], jobs = new Set(), scripts = new Map();
let sequence = 0;
const ids = new WeakMap();
let browser, context, page;
const {publicKey, privateKey} = crypto.generateKeyPairSync('ed25519');
const publicBytes = publicKey.export({format:'der',type:'spki'}).subarray(-32);
const address = bs58.encode(publicBytes);
function track(promise) { jobs.add(promise); promise.finally(()=>jobs.delete(promise)).catch(()=>{}); }
function saveScript(url, content, origin='browser') {
  if(scripts.has(url)) return scripts.get(url);
  const file = 'js/'+sha(url).slice(0,12)+'-'+path.basename(new URL(url).pathname).replace(/[^\w.~-]/g,'_');
  fs.writeFileSync(path.join(out,file),content);
  const item = {url:safeUrl(url),file,bytes:content.length,sha256:sha(content),origin};
  manifest.push(item); scripts.set(url,item); save('js-manifest.json',manifest); return item;
}
async function snapshot(label) {
  fs.writeFileSync(path.join(out,'snapshots',label+'.txt'),await page.locator('body').innerText());
  fs.writeFileSync(path.join(out,'snapshots',label+'.yaml'),await page.locator('body').ariaSnapshot());
  await page.screenshot({path:path.join(out,'snapshots',label+'.png'),fullPage:true});
  console.log(JSON.stringify({snapshot:label,url:page.url(),text:(await page.locator('body').innerText()).slice(0,18000),inputs:await page.locator('input').evaluateAll(es=>es.map(e=>({placeholder:e.placeholder,type:e.type,value:e.value}))),requests:requests.length,scripts:manifest.length}));
}
async function graph() {
  const queue = [...scripts.keys()], seen = new Set();
  const failures=[];
  while(queue.length) {
    const url = queue.shift(); if(seen.has(url))continue; seen.add(url);
    if(seen.size>2000 || manifest.reduce((n,x)=>n+x.bytes,0)>250_000_000) throw Error('Graph cap reached; capture incomplete');
    let item=scripts.get(url);
    if(!item) {
      try {
        const response=await context.request.get(url,{timeout:20000});
        if(!response.ok()){failures.push({url,status:response.status()});continue;}
        const content=await response.body(); if(content.length>15_000_000)throw Error('Script size cap');
        item=saveScript(url,content,'import-graph');
      }catch(e){failures.push({url,error:e.name});continue;}
    }
    const source=fs.readFileSync(path.join(out,item.file),'utf8');
    const matches=[...source.matchAll(/["'`]((?:\.\.?\/|\/assets\/|assets\/|https:\/\/)[^"'`\s<>]+\.m?js(?:\?[^"'`\s<>]*)?)["'`]/g)];
    for(const m of matches) {
      const base=m[1].startsWith('assets/')?'https://jup.ag/':url;
      const child=new URL(m[1],base).href;
      if(new URL(child).hostname==='jup.ag'&&!seen.has(child)&&!queue.includes(child))queue.push(child);
    }
  }
  save('graph-summary.json',{visited:seen.size,saved:manifest.length,failures,scope:'Observed browser JS plus all recursively discovered same-origin JS references; not every Jupiter product.'});
  console.log(JSON.stringify({graphScripts:manifest.length,failures:failures.length}));
}
async function main() {
  const proxy=proxySettings();
  browser=await pw.chromium.launch({headless:true,channel:'chrome',proxy,args:['--disable-quic','--force-webrtc-ip-handling-policy=disable_non_proxied_udp']});
  context=await browser.newContext({viewport:{width:1440,height:1100}});
  context.on('request', req=>{
    const id=++sequence; ids.set(req,id);
    const record={id,time:new Date().toISOString(),method:req.method(),url:safeUrl(req.url()),resourceType:req.resourceType(),headers:clean(req.headers()),body:body(req.postData())};
    requests.push(record); append('requests.jsonl',record);
  });
  context.on('requestfailed',req=>append('failures.jsonl',{id:ids.get(req),url:safeUrl(req.url()),error:req.failure()?.errorText}));
  context.on('response',response=>track((async()=>{
    const req=response.request(),id=ids.get(req),url=response.url();
    const headers=await response.allHeaders();
    const record={id,url:safeUrl(url),status:response.status(),headers:clean(headers)};
    try {
      const content=await response.body(); record.bytes=content.length;
      if(req.resourceType()==='script'||/\.m?js(?:\?|$)/.test(url))record.file=saveScript(url,content).file;
      else if(/json|text|javascript/.test(headers['content-type']||'')) {
        const text=content.toString('utf8');
        const file='responses/'+String(id).padStart(5,'0')+'.json';
        save(file,{url:safeUrl(url),status:record.status,body:body(text)}); record.file=file;
      }
    }catch(e){record.bodyError=e.name;}
    append('responses.jsonl',record);
  })()));
  await context.exposeBinding('__jupWallet',async (_,call)=>{
    append('wallet-events.jsonl',{time:new Date().toISOString(),...call});
    if(call.method==='signMessage') {
      const message=Buffer.from(call.bytes);
      if(message.length>4096||!message.toString('utf8').match(/jup|jupiter/i))throw Error('Disposable wallet only signs Jupiter auth messages');
      append('wallet-events.jsonl',{method:'signMessageResult',message:message.toString('utf8'),messageBase64:message.toString('base64'),signatureBytes:64,publicKey:address});
      return [...crypto.sign(null,message,privateKey)];
    }
  });
  await context.addInitScript(({address,bytes})=>{
    const events={}; const pk={toString:()=>address,toBase58:()=>address,toBytes:()=>new Uint8Array(bytes),toBuffer:()=>new Uint8Array(bytes)};
    const provider={isPhantom:true,isConnected:false,publicKey:null,
      on(name,fn){(events[name]??=[]).push(fn);return this;},off(name,fn){events[name]=(events[name]||[]).filter(f=>f!==fn);return this;},removeListener(name,fn){return this.off(name,fn);},
      async connect(options){await window.__jupWallet({method:'connect',options,publicKey:address}); this.publicKey=pk;this.isConnected=true;(events.connect||[]).forEach(f=>f(pk));return {publicKey:pk};},
      async disconnect(){this.publicKey=null;this.isConnected=false;await window.__jupWallet({method:'disconnect'});(events.disconnect||[]).forEach(f=>f());},
      async signMessage(message){const signed=await window.__jupWallet({method:'signMessage',bytes:[...message],message:new TextDecoder().decode(message)});return {signature:new Uint8Array(signed),publicKey:pk};},
      async signTransaction(){await window.__jupWallet({method:'signTransactionBlocked'});throw Error('Transaction signing disabled in capture');},
      async signAllTransactions(){throw Error('Transaction signing disabled in capture');},
      async signAndSendTransaction(){throw Error('Transaction submission disabled in capture');}
    };
    window.phantom={solana:provider};window.solana=provider;
  },{address,bytes:[...publicBytes]});
  page=await context.newPage();
  page.on('websocket',ws=>{
    append('websockets.jsonl',{event:'open',url:safeUrl(ws.url())});
    for(const name of ['framesent','framereceived'])ws.on(name,event=>append('websockets.jsonl',{event:name,url:safeUrl(ws.url()),payload:body(String(event.payload))}));
  });
  page.on('pageerror',e=>append('browser-errors.jsonl',{name:e.name,message:e.message.slice(0,300)}));
  save('capture-info.json',{startedAt:new Date().toISOString(),proxied:true,url:'https://jup.ag/swap/SOL-USDC',wallet:'disposable in-memory Ed25519; no funds; no transaction signing',address});
  await page.goto('https://jup.ag/swap/SOL-USDC',{waitUntil:'domcontentloaded',timeout:45000});
  await page.getByRole('button',{name:'Connect',exact:true}).first().waitFor({timeout:45000});
  await page.waitForTimeout(6000);
  await snapshot('01-loaded');
  const input=readline.createInterface({input:process.stdin});
  for await (const line of input) {
    try {
      const command=JSON.parse(line); append('actions.jsonl',{time:new Date().toISOString(),...command});
      if(command.action==='snapshot') await snapshot(command.label);
      else if(command.action==='click') {
        const locator=command.selector?page.locator(command.selector):page.getByRole(command.role||'button',{name:command.name,exact:command.exact??true});
        await locator.nth(command.index||0).click({timeout:12000});
        await page.waitForTimeout(command.waitMs||2500);await snapshot(command.label);
      } else if(command.action==='fill') {
        await page.locator(command.selector||'input').nth(command.index||0).fill(command.value);await page.waitForTimeout(command.waitMs||4000);await snapshot(command.label);
      } else if(command.action==='goto') {
        await page.goto(command.url,{waitUntil:'domcontentloaded',timeout:45000});await page.waitForTimeout(5000);await snapshot(command.label);
      } else if(command.action==='graph') await graph();
      else if(command.action==='finish')break;
      else throw Error('Unknown command');
    }catch(e){console.log(JSON.stringify({error:e.name,message:e.message.slice(0,600)}));}
  }
  await Promise.allSettled([...jobs]);
  save('capture-summary.json',{requests:requests.length,scripts:manifest.length,finishedAt:new Date().toISOString()});
  await context.close();await browser.close();console.log('Capture finished');
}
main().catch(async e=>{console.error('Jupiter capture stopped:',e.name,'; check proxy configuration and capture logs');await browser?.close();process.exitCode=1;});
