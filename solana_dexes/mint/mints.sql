CREATE TABLE IF NOT EXISTS solana_tokens (
    timestamp        TIMESTAMP,
    account          VARCHAR,
    transaction_hash VARCHAR,
    block_number     INTEGER,
    decimals         SMALLINT,
    mint_authority   VARCHAR
);