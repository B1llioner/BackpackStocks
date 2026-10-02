/* Explicit proxy settings shared by both Playwright Jupiter entry points. */
const names=['JUPITER_REPLAY_PROXY','JUPITER_PROXY_URL','OUTBOUND_PROXY_URL'];
function proxySettings(env=process.env) {
  const name=names.find(n=>env[n]&&env[n].trim());
  if(!name)throw Error('Set JUPITER_REPLAY_PROXY, JUPITER_PROXY_URL or OUTBOUND_PROXY_URL before networking');
  const value=env[name];
  if(value.trim().toLowerCase()==='direct')throw Error('Jupiter replay requires a proxy; DIRECT is disabled');
  try {
    const url=new URL(value);
    if(!['http:','https:'].includes(url.protocol)||!url.hostname||!['','/'].includes(url.pathname)||url.search||url.hash||/[\s\x00-\x1f]/.test(value)||url.port==='0')throw Error();
    const result={server:url.protocol+'//'+url.host,bypass:'<-loopback>'};
    if(url.username||url.password){result.username=decodeURIComponent(url.username);result.password=decodeURIComponent(url.password);}
    return result;
  }catch{throw Error('Proxy must be a valid HTTP(S) proxy URL');}
}
module.exports={proxySettings};
