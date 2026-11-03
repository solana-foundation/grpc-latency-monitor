# Solana Streaming Benchmark

Compares Yellowstone gRPC providers by when the same Solana events (accounts, transactions, slots) reach your subscriber: who delivers first, by how much, and who misses delivery deadlines. Results go to ClickHouse and Grafana.

> **Disclosure:** Built and maintained by Helius, one of the providers it can measure. Results favor providers close to the subscriber, so compare from more than one location.

## Quick start

Needs Rust, Docker, Yellowstone gRPC endpoints and a trusted Solana JSON-RPC endpoint for slot validation.

Each subscriber opens **five independent gRPC connections per configured provider
per location**: accounts and transactions at processed and finalized commitments,
plus one slot stream containing every slot status. Size provider connection limits
for all subscriber locations and any duplicate provider entries used for A/A tests.
The reference RPC is polled separately over HTTP.

```sh
docker compose up -d   # ClickHouse :8123, Grafana :3000 (override with CLICKHOUSE_PORT, GRAFANA_PORT)
cp config.example.yaml config.local.yaml   # set providers, filters, location; clickhouse.url: http://localhost:8123
cp .env.example .env                       # ClickHouse credentials, provider tokens, BENCH_REFERENCE_RPC_URL
cargo run --release -- --config config.local.yaml
```

Then open <http://localhost:3000>. For production deployments, see [Operations](docs/operations.md).

## Solana Foundation deployment

The numbers on [solana.com/data](https://solana.com/data) come from the Solana Foundation's deployment, which runs from [rpc-latency-monitor](https://github.com/solana-foundation/rpc-latency-monitor), not this repo. Its Ansible role builds this repo at a pinned commit and writes each host's config. The provider list is in [`deploy/ansible/roles/grpc_bench/defaults/main.yml`](https://github.com/solana-foundation/rpc-latency-monitor/blob/main/deploy/ansible/roles/grpc_bench/defaults/main.yml), and tokens are kept out of the repo.

| Provider | Endpoint |
| --- | --- |
| Helius | `https://laserstream-mainnet-<region>.helius-rpc.com`, the LaserStream region nearest each host |
| Triton | `https://riptide.rpcpool.com` |
| Alchemy | `https://solana-mainnet.streaming.alchemy.com` |
| QuickNode | Our QuickNode endpoint's host on `:443` |

To change an endpoint, open a PR against that file in rpc-latency-monitor.

## Docs

[Methodology](docs/methodology.md) · [Accuracy](docs/accuracy.md) · [Operations](docs/operations.md) · [Dashboards](docs/dashboards.md) · [Schema](sql/README.md) · [Example queries](sql/reports.sql) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md)

Licensed under [Apache-2.0](LICENSE).
