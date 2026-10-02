/* Parse every archived JS file; retain exact source offsets and formatted evidence. */
const fs=require('node:fs'),path=require('node:path');
const babel=require(process.env.JUPITER_BABEL_MODULE || 'playwright/lib/transform/babelBundle.js');
const root=path.resolve(process.argv[2]||'data/jupiter');
fs.mkdirSync(path.join(root,'formatted'),{recursive:true});
const manifest=JSON.parse(fs.readFileSync(path.join(root,'js-manifest.json')));
const result=[];
const patterns={http:/^(get|post|put|delete|patch|fetch|request|open)$/,wallet:/signMessage|signTransaction|signAllTransactions|signAndSend|signIn|connect|disconnect/,crypto:/subtle|digest|encrypt|decrypt|hmac|sha256|TextEncoder|atob|btoa/,flow:/getQuote|submitSwap|requestChallenge|verify|refresh|postAuthSession|rerollSwapAssignment|init/};
for(const item of manifest) {
 const raw=fs.readFileSync(path.join(root,item.file),'utf8');
 const row={...item,parsed:false,imports:[],endpoints:[],calls:[],definitions:[],role:[]};
 try {
  const ast=babel.babelParse(raw,path.basename(item.file)+'.js',true);
  row.parsed=true;row.parseErrors=(ast.errors||[]).map(e=>e.reasonCode);
  const formatted=babel.babelTransform(raw,path.basename(item.file)+'.js',true,[]);
  const formattedFile='formatted/'+path.basename(item.file);
  fs.writeFileSync(path.join(root,formattedFile),formatted.code);row.formatted=formattedFile;
  const strings=new Set();
  babel.traverse(ast,{
   ImportDeclaration(p){row.imports.push(p.node.source.value);},
   StringLiteral(p){if(/^https?:\/\/|^\/(?:v\d|auth|order|swap)|^auth\//.test(p.node.value))strings.add(p.node.value);},
   TemplateLiteral(p){const s=p.node.quasis.map(q=>q.value.raw).join('${…}');if(/https?:\/\/|\/auth\/|\/session|\/challenge|\/verify/.test(s))strings.add(s);},
   CallExpression(p){const n=p.node;let name=n.callee.property?.name||n.callee.property?.value||n.callee.name||'';const tags=Object.entries(patterns).filter(([,r])=>r.test(name)).map(([k])=>k);if(tags.length)row.calls.push({name,tags,offset:n.start,line:n.loc.start.line,snippet:raw.slice(n.start,Math.min(n.end,n.start+420))});},
   'ClassMethod|ObjectMethod|FunctionDeclaration'(p){const n=p.node;const name=n.key?.name||n.key?.value||n.id?.name||'';if(patterns.flow.test(name)||patterns.wallet.test(name))row.definitions.push({name,offset:n.start,line:n.loc.start.line,snippet:raw.slice(n.start,Math.min(n.end,n.start+800))});}
  });
  row.endpoints=[...strings].sort();
  if(row.calls.some(c=>c.tags.includes('wallet')))row.role.push('wallet/auth');
  if(row.calls.some(c=>c.tags.includes('http')))row.role.push('network');
  if(row.calls.some(c=>c.tags.includes('crypto')))row.role.push('crypto/encoding');
  if(/Swap|Trade|route/i.test(item.file))row.role.push('UI/routing');
  if(/vendor|runtime|polyfill/i.test(item.file))row.role.push('shared/vendor');
  if(!row.role.length)row.role.push('UI/data module; no selected network/signing calls');
 }catch(e){row.error={name:e.name,message:e.message.slice(0,250)};}
 result.push(row);
}
fs.writeFileSync(path.join(root,'file-analysis.json'),JSON.stringify(result,null,2));
const md=['# Разбор каждого сохранённого JS-файла','',`AST: Babel. Всего файлов: ${result.length}; разобраны: ${result.filter(r=>r.parsed).length}. Каждый файл прочитан и распарсен; выбранные сетевые/подписывающие вызовы имеют offsets в file-analysis.json. Это инвентаризация AST и предметный разбор цепочек, а не построчный аудит всех UI/vendor-модулей.`,'','| Файл | Назначение | Вызовы HTTP / wallet / crypto |','|---|---|---|'];
for(const r of result)md.push(`| [${path.basename(r.file)}](${r.formatted||r.file}) | ${r.role.join(', ')||r.error?.name} | ${['http','wallet','crypto'].map(tag=>r.calls.filter(c=>c.tags.includes(tag)).length).join(' / ')} |`);
fs.writeFileSync(path.join(root,'FILES.md'),md.join('\n')+'\n');
console.log(JSON.stringify({files:result.length,parsed:result.filter(r=>r.parsed).length,failed:result.filter(r=>!r.parsed).map(r=>({file:r.file,error:r.error}))}));
