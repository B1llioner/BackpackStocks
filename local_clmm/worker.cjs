// Pure local protocol math. This process receives public account bytes only.
const readline = require('node:readline');
const { PublicKey } = require('@solana/web3.js');
const BN = require('bn.js');
const noNetwork = () => { throw new Error('Math worker network disabled'); };
for (const name of ['node:http', 'node:https']) {
  const module = require(name); module.request = noNetwork; module.get = noNetwork;
}
require('node:net').connect = noNetwork;
require('node:net').createConnection = noNetwork;
require('node:tls').connect = noNetwork;
global.fetch = noNetwork;
const sdk = require('@raydium-io/raydium-sdk-v2');
const decode = (layout, data) => {
  const raw = Buffer.from(data, 'base64');
  if (raw.length !== layout.span) throw new Error('layout');
  return layout.decode(raw);
};
class QuoteRejection extends Error {
  constructor(code) { super(code); this.code = code; }
}
function calculate(q) {
  const poolInfo = decode(sdk.PoolInfoLayout, q.poolData);
  const extension = decode(sdk.TickArrayBitmapExtensionLayout, q.bitmapData);
  const poolId = new PublicKey(q.pool);
  if (!extension.poolId.equals(poolId)) throw new Error('bitmap pool');
  const common = { programId: new PublicKey(q.program), poolId,
    tickSpacing: poolInfo.tickSpacing, poolBitmap: poolInfo.tickArrayBitmap,
    tickArrayBitmap: extension,
    findInfo: { type: q.zeroForOne ? 'zeroForOne' : 'oneForZero',
      count: q.maxArrays, tickArrayCurrent: poolInfo.tickCurrent } };
  const addresses = sdk.TickArrayBitmapUtil.findTickArrayAddress(common).map(p => p.toBase58());
  if (q.op === 'addresses') return { addresses };
  if (q.op !== 'quote') throw new Error('operation');
  const tickArrays = q.arrays.map(a => {
    const value = decode(sdk.TickArrayLayout, a.data);
    if (!value.poolId.equals(poolId)) throw new Error('tick pool');
    return { address: new PublicKey(a.address), value };
  });
  if (tickArrays.length !== addresses.length || tickArrays.some((a, i) => a.address.toBase58() !== addresses[i])) {
    throw new QuoteRejection('clmm_input_mismatch');
  }
  const result = sdk.swapInternal({ programId: common.programId, poolId, poolInfo,
    tickArrays, configInfo: decode(sdk.ClmmConfigLayout, q.configData),
    tickarrayBitmapExtension: extension, amountSpecified: new BN(q.amount),
    sqrtPriceLimitX64: new BN(0), zeroForOne: q.zeroForOne, isBaseInput: true,
    blockTimestamp: q.timestamp, includeExtraTickArrays: false });
  if (!result.allTrade || !result.amountSpecifiedRemaining.isZero()) {
    const available = sdk.TickArrayBitmapUtil.findTickArrayAddress({ ...common,
      findInfo: { ...common.findInfo, count: undefined } });
    throw new QuoteRejection(addresses.length < available.length
      ? 'insufficient_tick_coverage' : 'insufficient_liquidity');
  }
  return { output: result.amountCalculated.toString(), fee: result.feeAmount.toString(),
    sqrtPrice: result.sqrtPriceX64.toString(), tick: result.tickCurrent,
    accounts: result.accounts.map(p => p.toBase58()) };
}
readline.createInterface({ input: process.stdin, crlfDelay: Infinity }).on('line', line => {
  let q;
  try {
    if (line.length > 2_000_000) throw new Error('request too large');
    q = JSON.parse(line);
    process.stdout.write(JSON.stringify({ id: q.id, result: calculate(q) }) + '\n');
  } catch (error) {
    const code = error instanceof QuoteRejection ? error.code : 'clmm_math_error';
    process.stdout.write(JSON.stringify({ id: q?.id, error: 'Local CLMM calculation rejected inputs', code }) + '\n');
  }
});
