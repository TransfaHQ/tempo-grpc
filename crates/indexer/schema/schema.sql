CREATE TABLE IF NOT EXISTS blocks
(
    number UInt64 CODEC(Delta, ZSTD),
    hash FixedString(32) CODEC(ZSTD),
    timestamp UInt64 CODEC(Delta, ZSTD),
    miner FixedString(20) CODEC(ZSTD),
    state_root FixedString(32) CODEC(ZSTD),
    receipts_root FixedString(32) CODEC(ZSTD),
    gas_limit UInt64 CODEC(ZSTD),
    gas_used UInt64 CODEC(ZSTD),
    extra_data String CODEC(ZSTD),
    nonce FixedString(8) CODEC(ZSTD),
    size UInt64 CODEC(ZSTD),
    is_deleted UInt8 DEFAULT 0,
    _version DateTime64(3) DEFAULT now64()
)
ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY (number)
PARTITION BY intDiv(number, 1000000)
PRIMARY KEY (number);

CREATE TABLE IF NOT EXISTS txs
(
    hash FixedString(32) CODEC(ZSTD),
    block_number UInt64 CODEC(Delta, ZSTD),
    block_timestamp UInt64 CODEC(Delta, ZSTD),
    tx_index UInt64 CODEC(Delta, ZSTD),
    from_address FixedString(20) CODEC(ZSTD),
    to_address Nullable(FixedString(20)) CODEC(ZSTD),
    value UInt256 CODEC(ZSTD),
    input String CODEC(ZSTD(3)),
    gas UInt64 CODEC(ZSTD),
    gas_price Nullable(UInt128) CODEC(ZSTD),
    nonce UInt64 CODEC(ZSTD),
    tx_type UInt8 CODEC(ZSTD),
    fee_token Nullable(FixedString(20)) CODEC(ZSTD),
    calls Array(Tuple(String, UInt256, Nullable(FixedString(20)))) CODEC(ZSTD(3)),
    is_deleted UInt8 DEFAULT 0,
    _version DateTime64(3) DEFAULT now64()
)
ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY (block_number, tx_index)
PRIMARY KEY (block_number, tx_index);

CREATE TABLE IF NOT EXISTS logs
(
    block_number UInt64 CODEC(Delta, ZSTD),
    block_timestamp UInt64 CODEC(Delta, ZSTD),
    transaction_hash FixedString(32) CODEC(ZSTD),
    transaction_index UInt64 CODEC(Delta, ZSTD),
    log_index UInt64 CODEC(Delta, ZSTD),
    address FixedString(20) CODEC(ZSTD),
    topics Array(FixedString(32)) CODEC(ZSTD),
    data String CODEC(ZSTD(3)),
    is_deleted UInt8 DEFAULT 0,
    _version DateTime64(3) DEFAULT now64()
)
ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY (block_number, transaction_index, log_index)
PRIMARY KEY (block_number, transaction_index, log_index);
