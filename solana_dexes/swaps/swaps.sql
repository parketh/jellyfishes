CREATE TABLE IF NOT EXISTS solana_swaps_raw (
    timestamp TIMESTAMP,
    dex TEXT,
    token_a TEXT,
    token_b TEXT,
    a_to_b BOOLEAN,
    amount_a DECIMAL,
    amount_b DECIMAL,
    account TEXT,
    block_number INTEGER,
    transaction_index SMALLINT,
    instruction_address SMALLINT[],
    transaction_hash TEXT,
    sign SMALLINT,
    PRIMARY KEY (block_number, transaction_index, transaction_hash, instruction_address)
);

CREATE TABLE IF NOT EXISTS solana_sync_status (
    id TEXT PRIMARY KEY,
    "offset" TEXT,
    initial TEXT
);