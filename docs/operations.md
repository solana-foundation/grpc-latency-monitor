# Operations

Installing, configuring and running a subscriber. To try it locally first, see the [quick start](../README.md#quick-start).

## Requirements

- Rust (see `rust-version` in `Cargo.toml`).
- Yellowstone gRPC endpoints that permit the configured subscriptions. Unrestricted account or transaction subscriptions need explicit opt-in and can have substantial bandwidth and ClickHouse storage cost; size and load-test before production.
- A trusted Solana HTTP(S) JSON-RPC endpoint for `getSlot` reference polling.
- ClickHouse with a database created for the benchmark, and a user authorized to insert into its tables.

## Configuration

Copy `config.example.yaml` to a local file (`*.local.yaml` and `*.local.yml` are ignored by Git) and set:

- account `owner` and transaction `account_include` public-key lists,
- provider endpoint URLs, and environment-variable **names** for their credentials,
- `slot_validation.rpc_url_env`, naming the environment variable holding the reference RPC URL,
- subscriber `region`, `cloud_provider`, `latitude` and `longitude` (all required).

The example contains no working endpoints or secrets. Run one instance and configuration per `(region, cloud_provider)` pair. Filter lists are sorted and deduplicated before building the same request for every provider; changing a list changes that stream's subscription ID, keeping historical comparisons separate.

Account streams always request `nonempty_txn_signature=true` and compare `(slot, pubkey, txn_signature)` at both commitments. Providers must support this filter and supply valid 64-byte transaction signatures. Unexpected missing, empty or malformed signatures use the normal malformed-update gap/reconnect path; there is no fallback to matching unrelated writes by account and slot. Updates without a modifying transaction, including ordinary startup snapshots, are outside this account workload. Multiple transactions touching one account now count as separate events, so recheck peak event rates and matcher capacity when upgrading from the old first-update-per-slot measurement. This starts new account configuration histories without a database migration.

Both filter lists must be nonempty by default, including when the `filters` block is omitted. To request unrestricted accounts, explicitly set `filters.accounts_owner: []` and `filters.allow_all_accounts: true`; unrestricted transactions independently require `filters.transactions_account_include: []` and `filters.allow_all_transactions: true`. Both opt-ins default to false. Each list accepts at most 100 entries, counting duplicates before normalization. Public keys must encode 32 bytes in 32–44 base58 characters. Providers may impose smaller limits. The opt-ins do not change nonempty filter behavior, votes remain included. `filters.transactions_failed` is unset by default, which includes failed and successful transactions; `false` excludes failed transactions and `true` requests only failed ones. Changing it starts new transaction subscription versions.

Each subscriber opens five gRPC connections per configured provider at its location: two account commitments, two transaction commitments and one all-status slot stream. Across N provider entries and L subscriber locations, allow 5 × N × L connections, including A/A entries. Slot notifications are stored under `processed` regardless of their actual status; dashboard commitment selection applies to accounts and transactions, while both views use the single slot stream. Historical duplicates stored under `finalized` are excluded by dashboard scopes. The independent reference RPC uses HTTP polling in addition to these gRPC connections.

Account subscriptions request full account data without data slices, preserving the input for future consistency checks.

Latitude and longitude are your subscriber's city or datacenter coordinates; missing, null, non-finite or out-of-range values prevent startup. For example:

```yaml
subscriber:
  region: "FRA2"
  cloud_provider: "example-cloud"
  latitude: 50.1109
  longitude: 8.6821
```

## Credentials

The benchmark loads `.env` from the current working directory before reading configuration. Copy `.env.example` to `.env` and set your ClickHouse credentials, provider tokens and `BENCH_REFERENCE_RPC_URL` there. Already exported environment variables take precedence. A missing `.env` is allowed; an unreadable or malformed file causes a startup error. Never put provider tokens or RPC URLs containing API keys in log messages.

## Slot reference

`slot_validation` is required. Existing configurations must add this block and set the named environment variable:

```yaml
slot_validation:
  rpc_url_env: BENCH_REFERENCE_RPC_URL
  slot_buffer: 25
```

One background task polls `getSlot` at processed, confirmed and finalized commitments, making three requests per poll. Optional settings are `poll_interval` (default `1s`), `request_timeout` (`2s`) and `max_reference_age` (`5s`). All durations must be positive, and the maximum age must exceed both the polling interval and request timeout. Reference age starts when requests start, so network delays consume the freshness budget. Redirects are rejected, responses are bounded, and RPC error bodies and URLs are omitted from diagnostics.

Each account or transaction stream uses its commitment's reference. Slot notifications use their actual status, since the single slot subscription receives every status; confirmed and finalized statuses use their respective tips, and the other statuses use processed. The inclusive permitted range for new comparisons is reference ± `slot_buffer`, with saturation at integer limits. A peer of an already-open comparison can still arrive after the lower boundary advances, preserving its original deadline; this cannot reopen closed keys and does not bypass reference outages. Validate the default 25-slot allowance against live traffic before publishing results. Rejections are logged as per-provider, per-stream stale/future/reference-unavailable counts once per tick when nonzero. Rejected keys use the existing matcher window and deduplication limits to suppress incomplete comparisons if a later copy passes admission. No new tracking configuration is needed. Only keys seen in bounds can advance replay floors; rejected-only markers expire without trusting their slots. Rejection flags are tracked per provider and reference commitment. When every connected provider is out of bounds against the same tip, the stream invalidates open comparisons and certifies no coverage: the reference, not the providers, is presumed wrong. Accepted processed slot updates cannot clear confirmed or finalized flags; only an in-bounds update using the same tip, or a reconnect, clears a provider's flag. A peer admitted below the lower boundary of an already-open comparison does not clear it. Dispute transitions are recorded immediately, even between certification ticks, and resumed coverage cannot start before recovery. This is a local measurement gap and does not change provider downtime. An update delayed in the matcher beyond `processing_grace` ends healthy coverage before slot bounds are checked, so a matcher backlog is not counted as a provider's stale update.

Until initial references arrive, and whenever they expire, scoring and healthy coverage stop. Open comparisons affected by an RPC gap are invalidated. Recovery starts a new healthy interval; it does not change provider connection history or count as provider downtime. Each poll keeps the highest tips seen per commitment, so a load-balanced backend slightly behind the last answer does not move the reference backwards. A poll regressing by more than `slot_buffer` on any commitment is refused. Only a successful poll whose processed tip advanced renews freshness, so a stuck backend expires the reference instead of rejecting live slots. The RPC is still trusted for its tip values; it is not an independent proof of ancestry. Choose a reliable reference on the same network as the measured providers.

The validation policy and a hash of the reference RPC URL enter all five active configuration IDs. This starts new measurement histories, including when the reference URL changes. The raw URL and credentials are never stored in configuration descriptions. The comparison window, processing grace, connection warmup and slot-confirmation timeout retain their existing meanings.

## ClickHouse

Set `clickhouse.url` to the external ClickHouse HTTP(S) endpoint (typically port 8123, not the native port 9000). Create the database and apply the [Goose migrations](../sql/README.md) with a separate migration user. The application user needs:

- INSERT and DESCRIBE on the five ingestion/metadata tables and `benchmark_dirty_minutes`,
- SELECT on event lags, dirty minutes and rollup commits,
- INSERT on minute rollups and rollup commits,
- ALTER DELETE on minute rollups (to delete superseded summary generations).

Retention is managed in the SQL migrations: six hours of raw measurements and 30 days of minute summaries and connection/health history. Each subscriber builds its own [minute summaries](../sql/README.md#hybrid-dashboard-summaries) in the background; no separate rollup service is needed.

`state_dir` defaults to `.benchmark-state` in the working directory. Keep it on persistent storage and mount it into subscriber containers. It stores only pending minute IDs, under filenames hashed from database and subscriber identity. The writer syncs this recovery metadata before attempting raw inserts and clears it after all batch inserts are acknowledged. Startup marks any remaining minutes unknown in ClickHouse before starting rollups or provider subscriptions; if recovery fails, startup stops. Preserve the directory when restarting or moving the subscriber.

## Running

```sh
cargo run --release -- --config your.local.yaml
```

Startup checks, using the Rust row structs, that the written columns exist and are insertable before opening provider streams. This metadata check does not test INSERT privileges; see [Startup validation](../sql/README.md#startup-validation).

Allow at least 60 seconds for matchers to drain, the writer's remaining insert timeouts, and 150 seconds for final rollups when setting termination grace. A matcher timeout or failed writer produces an unsuccessful exit; only a clean ingestion shutdown publishes the final open minute. Alert on unmatched expected-row counts or a positive `incomplete` count in `benchmark_dirty_minutes`: affected minutes stay unknown after restart and require investigation, not blind insert retries.

## Tuning and connection behavior

Stream readers run on their own Tokio runtime, separate from matching, the ClickHouse writer and summaries, so that work never delays a timestamp.

Configurable: `window`, `processing_grace`, `expired_key_retention`, `expired_key_capacity`, `connection_warmup`, `slot_confirmation_timeout`, `matcher_key_capacity`, queue sizes, batch size and flush interval. Set `window` to the latest delivery you still consider on time. Size `matcher_key_capacity` for at least your peak unique events per second per stream × (`window` + `processing_grace` + 1 second). Keys beyond the capacity are dropped (and remembered), with one unknown-coverage marker per second.

Connections:

- Adaptive HTTP/2 receive windows let flow control adjust to connection throughput and latency.
- HTTP/2 keepalive pings (every 10 seconds, 10-second acknowledgement timeout) detect dead connections. The reader also pings every 10 seconds and reconnects a stream that receives no update, ping or pong for 30 seconds.
- Subscriptions time out after 10 seconds. Messages up to 64 MiB are accepted (tonic's default is 4 MiB; accounts can hold up to 10 MiB).
- A subscription that stayed up for 30 seconds reconnects immediately after it drops, unless the disconnect was caused by the subscriber's own backlog. Until a subscription stays up that long again, the wait between attempts follows the time since the outage began: every second for the first minute, every 15 seconds until 15 minutes, then every minute, so a long outage is not retried constantly. An attempt in progress at the provider's recovery can still run until its timeouts (10 seconds to connect, 10 seconds to subscribe) before that wait begins, so a recorded outage end can lag the recovery by up to one wait plus 20 seconds.

## Reports

See `sql/reports.sql` for sample head-to-head, leaderboard, connectivity, regional and coverage queries. In a head-to-head comparison, negative `lag_A − lag_B` means provider A arrived first. Do not calculate this by subtracting separately aggregated medians.
