const test=require('node:test'),assert=require('node:assert/strict');
const {proxySettings}=require('../scripts/jupiter_proxy.cjs');
test('Playwright proxy splits percent-encoded credentials and covers loopback',()=>{
 assert.deepEqual(proxySettings({OUTBOUND_PROXY_URL:'http://fixture%40user:fixture%3Apass@proxy.example:8080'}),{
  server:'http://proxy.example:8080',bypass:'<-loopback>',username:'fixture@user',password:'fixture:pass'
 });
});
test('same proxy precedence as Python',()=>{
 assert.equal(proxySettings({JUPITER_REPLAY_PROXY:'http://preferred:80',JUPITER_PROXY_URL:'http://other:80'}).server,'http://preferred');
 assert.equal(proxySettings({JUPITER_PROXY_URL:'https://proxy.example:8443'}).server,'https://proxy.example:8443');
});
test('invalid or absent proxy never returns direct config or reveals credentials',()=>{
 for(const value of [undefined,'DIRECT','socks5://proxy:1080','http://user:secret@host:99999','http://user:secret@host/path','http://host:0']) {
  assert.throws(()=>proxySettings({JUPITER_REPLAY_PROXY:value}),e=>!e.message.includes('secret'));
 }
});
