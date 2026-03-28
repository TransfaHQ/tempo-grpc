tempo-grpc
======

A [Reth](https://reth.rs) Execution Extension that streams blocks, transactions, and logs over gRPC for fast blockchain indexing on [Tempo](https://tempo.xyz).

## Why?

Blockchain indexing is essentially an ETL pipeline over a stream of immutable temporal events. The polling approach that JSON-RPC forces doesn't fit this model well. gRPC streams do. Blocks can be encoded with Protocol Buffers and streamed to clients over a long-lived HTTP/2 connection, which gives us:

- **Less bandwidth** at scale. Protobuf is smaller and faster to serialize than JSON, and its binary format works well with HTTP/2's binary framing.
- **No polling latency.** New data is pushed to clients as soon as it's available instead of requiring repeated HTTP roundtrips.
- **Built-in reorg handling.** Blocks are streamed with a status flag (committed or reorged), so clients can react to reorgs without tracking chain state themselves.
- **Backpressure for free.** HTTP/2 flow control means the stream slows down if the client falls behind (e.g. slow database writes), instead of buffering without bounds or dropping data.

## How it works

We built this on top of Tempo's Reth node using an [Execution Extension](https://reth.rs/exex/overview/) to get notified when new blocks are committed or reorged. The gRPC server runs alongside the node via `spawn_critical_task` and exposes three methods:

```proto
service BlockStream {
  rpc Live(LiveRequest) returns (stream BlockChunk) {}
  rpc Backfill(BackfillRequest) returns (stream BlockChunk) {}
  rpc BackfillToLive(BackfillToLiveRequest) returns (stream BlockChunk) {}
}
```

- **`Live`** subscribes to new blocks as they are committed (or reorged).
- **`Backfill`** streams a historical range of blocks.
- **`BackfillToLive`** backfills from a given block, then transitions to a live stream.

You can read more about it [here](https://transfa.com/blog/reth-grpc-streaming-for-indexing).

## Setup

### Prerequisites

- [Rust](https://rustup.rs/) (nightly, 2024 edition)
- Protocol Buffers compiler
- C/C++ build toolchain

On Ubuntu/Debian:

```bash
sudo apt install build-essential protobuf-compiler clang pkg-config libssl-dev
```

On macOS:

```bash
brew install protobuf
```

### Building

```bash
cargo build --release
```

The binary will be at `target/release/tempo-grpc`.

### Running the node

```bash
tempo-grpc node \
  --follow \
  --http --http.port 8545 \
  --http.api eth,net,web3,txpool,trace \
  --grpc.addr 127.0.0.1 \
  --grpc.port 50051
```

`--grpc.addr` defaults to `127.0.0.1` and `--grpc.port` defaults to `50051`. All standard Tempo node options apply. Refer to the [Tempo node guide](https://docs.tempo.xyz/guide/node/rpc) for running an RPC node.

### Running the indexer

The repo includes a basic gRPC client that streams blocks from the server and inserts them into ClickHouse. It's mainly useful for testing and benchmarking the gRPC server. It requires a running ClickHouse instance.

```bash
cargo run --release -p indexer -- \
  --grpc_url http://localhost:50051 \
  --from 0 \
  --to 1000000 \
  --batch_size 100 \
  --concurrency 4 \
  --ch_url http://localhost:8123 \
  --ch_database default \
  --ch_user default \
  --ch_password default
```

If `--to` is omitted, the indexer backfills from `--from` and then transitions to a live stream. `--batch_size` controls how many blocks are sent per gRPC message, and `--concurrency` sets the number of parallel workers inserting into ClickHouse.

## Benchmarks

To put this to the test, a backfill was run for blocks 9,000,000 to 10,000,000 on the Tempo testnet. The client ran on a 4 vCPU, 16 GB RAM server in the same region as the node, connected over a 10 Gbps link.

| Metric | Rows | Size |
|---|---|---|
| Blocks | 1,000,000 | 158.22 MiB |
| Transactions | 36.04M | 13.70 GiB |
| Logs | 96.13M | 21.28 GiB |
| **Total** | | **~35.14 GiB** |
| **Total time** | | **293s** |

That works out to roughly 3K blocks, 123K transactions, and 328K logs per second, or about 122 MiB/s of uncompressed data on modest hardware.

The same backfill was also run from a machine in Europe against the node in Canada to test performance over a cross-region network. The client was an AMD Ryzen PRO 3600 (6C/12T, 3.6 GHz) with 32 GB RAM and a 500 Mbps connection. Despite the transatlantic latency, the backfill completed in ~548s (roughly 1.9x slower). Most of the difference is attributable to the lower bandwidth and added network round trips.
