-- Additive schema in a dedicated database; migration role owns all objects.
-- Raw token amounts and Q64 values must never be floating-point columns.
CREATE SCHEMA IF NOT EXISTS market_data;
CREATE TABLE IF NOT EXISTS market_data.schema_migrations (
  version integer PRIMARY KEY CHECK (version > 0),
  checksum text NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE market_data.scan_runs (
  id uuid PRIMARY KEY,
  started_at timestamptz NOT NULL,
  finished_at timestamptz,
  status text NOT NULL CHECK (status IN ('running','complete','partial','failed')),
  summary jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(summary) = 'object')
);
CREATE TABLE market_data.tokens (
  mint text PRIMARY KEY CHECK (length(mint) BETWEEN 32 AND 44),
  decimals smallint CHECK (decimals BETWEEN 0 AND 255),
  token_program text,
  chain_verified boolean NOT NULL DEFAULT false,
  slot bigint CHECK (slot >= 0),
  info jsonb NOT NULL CHECK (jsonb_typeof(info) = 'object'),
  observed_at timestamptz NOT NULL
);
CREATE TABLE market_data.equities (
  symbol text PRIMARY KEY CHECK (symbol ~ '^[A-Z0-9._-]+\.US$'),
  mint text NOT NULL REFERENCES market_data.tokens(mint),
  decimals smallint NOT NULL CHECK (decimals BETWEEN 0 AND 18),
  metadata jsonb NOT NULL CHECK (jsonb_typeof(metadata) = 'object'),
  observed_at timestamptz NOT NULL
);
CREATE TABLE market_data.pools (
  address text PRIMARY KEY CHECK (length(address) BETWEEN 32 AND 44),
  program text,
  kind text NOT NULL CHECK (kind IN ('clmm','cpmm','amm_v4','other','unverified')),
  mint_a text REFERENCES market_data.tokens(mint),
  mint_b text REFERENCES market_data.tokens(mint),
  venue text NOT NULL,
  chain_verified boolean NOT NULL,
  discovery jsonb NOT NULL CHECK (jsonb_typeof(discovery) = 'object'),
  reason text,
  observed_at timestamptz NOT NULL,
  CHECK (mint_a IS NULL OR mint_b IS NULL OR mint_a <> mint_b)
);
CREATE INDEX pools_pair_idx ON market_data.pools(mint_a,mint_b);
CREATE TABLE market_data.asset_pools (
  symbol text NOT NULL REFERENCES market_data.equities(symbol),
  pool_address text NOT NULL REFERENCES market_data.pools(address),
  source text NOT NULL CHECK (source IN ('raydium_index','jupiter_route')),
  observed_at timestamptz NOT NULL,
  PRIMARY KEY(symbol,pool_address,source)
);
CREATE TABLE market_data.pool_snapshots (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  scan_id uuid NOT NULL REFERENCES market_data.scan_runs(id),
  pool_address text NOT NULL REFERENCES market_data.pools(address),
  slot bigint NOT NULL CHECK (slot >= 0),
  digest text NOT NULL,
  observed_at timestamptz NOT NULL,
  price_b_per_a numeric CHECK (price_b_per_a > 0),
  reserve_a numeric(78,0) CHECK (reserve_a >= 0),
  reserve_b numeric(78,0) CHECK (reserve_b >= 0),
  sqrt_price_x64 numeric(78,0) CHECK (sqrt_price_x64 >= 0),
  liquidity numeric(78,0) CHECK (liquidity >= 0),
  state jsonb NOT NULL CHECK (jsonb_typeof(state) = 'object'),
  quote_supported boolean NOT NULL,
  reason text,
  UNIQUE(pool_address,slot,digest)
);
CREATE INDEX pool_snapshots_latest_idx ON market_data.pool_snapshots(pool_address,observed_at DESC,id DESC);
CREATE TABLE market_data.quotes (
  id uuid PRIMARY KEY,
  scan_id uuid NOT NULL REFERENCES market_data.scan_runs(id),
  symbol text NOT NULL REFERENCES market_data.equities(symbol),
  source text NOT NULL CHECK (source IN ('local_cpmm','jupiter')),
  pool_address text REFERENCES market_data.pools(address),
  input_mint text NOT NULL REFERENCES market_data.tokens(mint),
  output_mint text NOT NULL REFERENCES market_data.tokens(mint),
  input_raw numeric(78,0) NOT NULL CHECK (input_raw > 0),
  output_raw numeric(78,0) NOT NULL CHECK (output_raw > 0),
  minimum_output_raw numeric(78,0) CHECK (minimum_output_raw >= 0),
  observed_at timestamptz NOT NULL,
  slot bigint CHECK (slot >= 0),
  details jsonb NOT NULL CHECK (jsonb_typeof(details) = 'object'),
  CHECK (input_mint <> output_mint)
);
CREATE INDEX quotes_size_latest_idx ON market_data.quotes(symbol,input_mint,output_mint,input_raw,observed_at DESC,id);
GRANT USAGE ON SCHEMA market_data TO backpack_catalog;
GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA market_data TO backpack_catalog;
REVOKE INSERT,UPDATE ON market_data.schema_migrations FROM backpack_catalog;
GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA market_data TO backpack_catalog;
