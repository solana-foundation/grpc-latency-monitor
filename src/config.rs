//! YAML configuration. Credentials are read from the environment, never from YAML.

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::{collections::HashSet, path::Path, time::Duration};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Persist this directory across restarts; it contains pending-write metadata.
    #[serde(default = "default_state_dir")]
    pub state_dir: std::path::PathBuf,
    pub subscriber: SubscriberConfig,
    pub providers: Vec<ProviderConfig>,
    pub slot_validation: SlotValidationConfig,
    pub clickhouse: ClickHouseConfig,
    #[serde(default)]
    pub filters: FiltersConfig,
    pub batch_size: usize,
    pub arrival_buffer_capacity: usize,
    pub storage_buffer_capacity: usize,
    pub matcher_key_capacity: usize,
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
    /// The only comparison window, measured from the first provider's delivery.
    /// Deliveries strictly inside it are on time; anything later counts as
    /// a missed deadline, even if it arrives afterwards.
    #[serde(with = "humantime_serde")]
    pub window: Duration,
    #[serde(with = "humantime_serde")]
    pub processing_grace: Duration,
    /// How long closed (or capacity-rejected) event keys are remembered, so a
    /// delivery after the window is ignored instead of starting a new comparison
    /// in which the late provider wins and punctual providers miss.
    #[serde(default = "default_expired_key_retention", with = "humantime_serde")]
    pub expired_key_retention: Duration,
    /// Closed keys remembered exactly, per stream. Defaults to enough to cover
    /// `expired_key_retention` at the rate `matcher_key_capacity` is sized for;
    /// forgotten keys are still rejected by a slot floor, including normal expiry.
    #[serde(default)]
    expired_key_capacity: Option<usize>,
    /// After each subscription, how long before a provider can record a missed
    /// deadline and before healthy coverage starts. Zero disables the warmup.
    #[serde(default = "default_connection_warmup", with = "humantime_serde")]
    pub connection_warmup: Duration,
    /// How long a processed miss waits for its slot to be confirmed or dead.
    /// Undecided misses then count as forked, not a missed deadline.
    #[serde(
        default = "default_slot_confirmation_timeout",
        with = "humantime_serde"
    )]
    pub slot_confirmation_timeout: Duration,
    /// Removed setting, accepted only to explain the migration at startup.
    #[serde(default, rename = "late_period")]
    removed_late_period: Option<serde::de::IgnoredAny>,
}

/// Reference RPC credentials stay in the environment. Tuning is optional.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotValidationConfig {
    pub rpc_url_env: String,
    #[serde(default = "default_slot_buffer")]
    pub slot_buffer: u64,
    #[serde(default = "default_poll_interval", with = "humantime_serde")]
    pub poll_interval: Duration,
    #[serde(default = "default_request_timeout", with = "humantime_serde")]
    pub request_timeout: Duration,
    #[serde(default = "default_reference_age", with = "humantime_serde")]
    pub max_reference_age: Duration,
}

fn default_slot_buffer() -> u64 {
    25
}
fn default_poll_interval() -> Duration {
    Duration::from_secs(1)
}
fn default_request_timeout() -> Duration {
    Duration::from_secs(2)
}
fn default_reference_age() -> Duration {
    Duration::from_secs(5)
}

impl SlotValidationConfig {
    pub fn rpc_url(&self) -> Result<String> {
        credential(&self.rpc_url_env, "reference RPC URL")?;
        let value = std::env::var(&self.rpc_url_env)
            .map_err(|_| anyhow::anyhow!("reference RPC URL must be valid Unicode"))?;
        let url = reqwest::Url::parse(&value)
            .map_err(|_| anyhow::anyhow!("reference RPC URL must be an HTTP(S) URL"))?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            bail!("reference RPC URL must be an HTTP(S) URL");
        }
        Ok(value)
    }

    fn validate(&self) -> Result<()> {
        self.rpc_url()?;
        if self.poll_interval.is_zero()
            || self.request_timeout.is_zero()
            || self.max_reference_age <= self.poll_interval
            || self.max_reference_age <= self.request_timeout
        {
            bail!("reference RPC durations must be positive; max_reference_age must exceed poll_interval and request_timeout");
        }
        Ok(())
    }
}

fn default_state_dir() -> std::path::PathBuf {
    ".benchmark-state".into()
}

fn default_expired_key_retention() -> Duration {
    Duration::from_secs(120)
}

fn default_connection_warmup() -> Duration {
    Duration::from_secs(1)
}

fn default_slot_confirmation_timeout() -> Duration {
    Duration::from_secs(30)
}

/// Per-list cap, including duplicate entries. Providers may impose smaller limits.
pub const MAX_FILTER_ADDRESSES: usize = 100;

/// Yellowstone filters shared by every provider at both commitments.
/// Empty lists require a separate explicit opt-in for each data type.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FiltersConfig {
    pub accounts_owner: Vec<String>,
    pub transactions_account_include: Vec<String>,
    pub allow_all_accounts: bool,
    pub allow_all_transactions: bool,
    /// Yellowstone `failed`: None sends all, false only successful, true only failed.
    pub transactions_failed: Option<bool>,
}

impl FiltersConfig {
    /// Order and duplicates must not change the request or its subscription ID.
    pub fn normalized(&self) -> Self {
        let mut result = self.clone();
        result.accounts_owner.sort_unstable();
        result.accounts_owner.dedup();
        result.transactions_account_include.sort_unstable();
        result.transactions_account_include.dedup();
        result
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriberConfig {
    pub region: String,
    pub cloud_provider: String,
    pub latitude: f64,
    pub longitude: f64,
}

impl SubscriberConfig {
    pub fn validate(&self) -> Result<()> {
        nonempty(&self.cloud_provider, "subscriber cloud provider")?;
        nonempty(&self.region, "subscriber region")?;
        if !self.latitude.is_finite()
            || !self.longitude.is_finite()
            || !(-90.0..=90.0).contains(&self.latitude)
            || !(-180.0..=180.0).contains(&self.longitude)
        {
            bail!("subscriber coordinates must be finite: latitude in [-90, 90], longitude in [-180, 180]");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    pub name: String,
    pub url: String,
    pub token_env: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub user_env: String,
    pub password_env: String,
}

impl Config {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let text = std::fs::read_to_string(path).context("reading configuration")?;
        Self::from_yaml(&text)
    }

    pub fn from_yaml(text: &str) -> Result<Self> {
        let config: Self = yaml_serde::from_str(text).context("parsing YAML configuration")?;
        config.validate()?;
        Ok(config)
    }

    /// `matcher_key_capacity` holds about `window + processing_grace + 1s` of
    /// keys, so the default keeps that rate's keys for the whole retention.
    pub fn expired_key_capacity(&self) -> usize {
        self.expired_key_capacity.unwrap_or_else(|| {
            let span = self.window + self.processing_grace + Duration::from_secs(1);
            let spans = self.expired_key_retention.as_secs_f64() / span.as_secs_f64();
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let spans = (spans.ceil() as usize).max(2);
            self.matcher_key_capacity.saturating_mul(spans)
        })
    }

    pub fn validate(&self) -> Result<()> {
        self.slot_validation.validate()?;
        if self.removed_late_period.is_some() {
            bail!(
                "`late_period` was removed: a delivery after `window` now counts as a missed deadline \
                 (missed). Delete `late_period` and set `window` to the latest delivery you \
                 still consider on time (for example \"5s\")"
            );
        }
        if self.providers.len() < 2 {
            bail!("at least two providers are required");
        }
        let mut names = HashSet::new();
        for provider in &self.providers {
            nonempty(&provider.name, "provider name")?;
            // Names are listed in configuration descriptions and dashboard labels.
            if provider.name.contains([';', ',', '"', '=', '@']) {
                bail!("provider names must not contain ; , \" = or @");
            }
            nonempty(&provider.url, "provider URL")?;
            if !names.insert(&provider.name) {
                bail!("provider names must be distinct");
            }
            if let Some(variable) = &provider.token_env {
                credential(variable, "provider token")?;
            }
        }
        self.subscriber.validate()?;
        nonempty(&self.clickhouse.url, "ClickHouse URL")?;
        nonempty(&self.clickhouse.database, "ClickHouse database")?;
        credential(&self.clickhouse.user_env, "ClickHouse user")?;
        credential(&self.clickhouse.password_env, "ClickHouse password")?;
        for (name, keys, allow_all, opt_in) in [
            (
                "filters.accounts_owner",
                &self.filters.accounts_owner,
                self.filters.allow_all_accounts,
                "filters.allow_all_accounts",
            ),
            (
                "filters.transactions_account_include",
                &self.filters.transactions_account_include,
                self.filters.allow_all_transactions,
                "filters.allow_all_transactions",
            ),
        ] {
            if keys.is_empty() && !allow_all {
                bail!("{name} must not be empty unless {opt_in} is true");
            }
            if keys.len() > MAX_FILTER_ADDRESSES {
                bail!("{name} exceeds the limit of {MAX_FILTER_ADDRESSES} entries");
            }
            for key in keys {
                if !(32..=44).contains(&key.len()) {
                    bail!("{name} must contain 32-byte Solana public keys encoded in 32 to 44 base58 characters");
                }
                let decoded = bs58::decode(key)
                    .into_vec()
                    .with_context(|| format!("{name} contains an invalid base58 public key"))?;
                if decoded.len() != 32 {
                    bail!("{name} must contain 32-byte Solana public keys");
                }
            }
        }
        if self.batch_size == 0
            || self.arrival_buffer_capacity == 0
            || self.storage_buffer_capacity == 0
            || self.matcher_key_capacity == 0
            || self.expired_key_capacity == Some(0)
            || self.flush_interval.is_zero()
            || self.window.is_zero()
            || self.processing_grace.is_zero()
            || self.expired_key_retention.is_zero()
            || self.slot_confirmation_timeout.is_zero()
        {
            bail!("batching, buffer capacities and durations must be positive");
        }
        Ok(())
    }
}

fn nonempty(value: &str, label: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{label} must not be empty");
    }
    Ok(())
}

fn credential(variable: &str, label: &str) -> Result<()> {
    nonempty(variable, &format!("{label} environment variable name"))?;
    if !variable.bytes().enumerate().all(|(index, byte)| {
        byte.is_ascii_alphabetic() || byte == b'_' || (index > 0 && byte.is_ascii_digit())
    }) {
        bail!("{label} environment variable name is invalid");
    }
    if std::env::var_os(variable).is_none_or(|value| value.is_empty()) {
        bail!("{label} environment variable is unset or empty");
    }
    Ok(())
}

/// The example configuration, with test credentials set in the environment.
#[cfg(test)]
impl Config {
    pub(crate) fn example() -> Self {
        std::env::set_var(
            "BENCH_REFERENCE_RPC_URL",
            "https://reference.invalid/private-test-key",
        );
        std::env::set_var("INTEGRATION_CH_USER", "test");
        std::env::set_var("INTEGRATION_CH_PASSWORD", "test");
        std::env::set_var("BENCH_PROVIDER_A_TOKEN", "test");
        Self::from_yaml(
            &include_str!("../config.example.yaml")
                .replace("BENCH_CH_USER", "INTEGRATION_CH_USER")
                .replace("BENCH_CH_PASSWORD", "INTEGRATION_CH_PASSWORD"),
        )
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example() -> String {
        std::env::set_var(
            "BENCH_REFERENCE_RPC_URL",
            "https://reference.invalid/private-test-key",
        );
        std::env::set_var("BENCH_TEST_CH_USER", "test");
        std::env::set_var("BENCH_TEST_CH_PASSWORD", "test");
        std::env::set_var("BENCH_TEST_PROVIDER_TOKEN", "secret");
        include_str!("../config.example.yaml")
            .replace("BENCH_CH_USER", "BENCH_TEST_CH_USER")
            .replace("BENCH_CH_PASSWORD", "BENCH_TEST_CH_PASSWORD")
            .replace("BENCH_PROVIDER_A_TOKEN", "BENCH_TEST_PROVIDER_TOKEN")
    }

    #[test]
    fn unrestricted_filters_require_independent_explicit_opt_ins() {
        let mut config = Config::example();
        assert!(!config.filters.allow_all_accounts && !config.filters.allow_all_transactions);
        config.filters.accounts_owner.clear();
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("filters.allow_all_accounts"));
        config.filters.allow_all_transactions = true;
        assert!(
            config.validate().is_err(),
            "transaction opt-in cannot allow all accounts"
        );
        config.filters.allow_all_accounts = true;
        config.validate().unwrap();
        config.filters.transactions_account_include.clear();
        config.filters.allow_all_transactions = false;
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("filters.allow_all_transactions"));
        config.filters.allow_all_transactions = true;
        config.validate().unwrap();
        let yaml = example();
        let (prefix, rest) = yaml.split_once("filters:\n").unwrap();
        let (_, subscriber) = rest.split_once("subscriber:\n").unwrap();
        let without_filters = format!("{prefix}subscriber:\n{subscriber}");
        assert!(
            Config::from_yaml(&without_filters).is_err(),
            "omitting filters cannot request everything implicitly"
        );
        let explicit = format!("{prefix}filters:\n  accounts_owner: []\n  transactions_account_include: []\n  allow_all_accounts: true\n  allow_all_transactions: true\nsubscriber:\n{subscriber}");
        Config::from_yaml(&explicit).unwrap();
    }

    #[test]
    fn filter_lists_are_bounded_even_with_an_unrestricted_opt_in() {
        let base = Config::example();
        let key = base.filters.accounts_owner[0].clone();
        for accounts in [true, false] {
            let mut config = base.clone();
            let filters = &mut config.filters;
            filters.allow_all_accounts = true;
            filters.allow_all_transactions = true;
            let list = if accounts {
                &mut filters.accounts_owner
            } else {
                &mut filters.transactions_account_include
            };
            *list = vec![key.clone(); MAX_FILTER_ADDRESSES];
            config.validate().unwrap();
            let list = if accounts {
                &mut config.filters.accounts_owner
            } else {
                &mut config.filters.transactions_account_include
            };
            list.push(key.clone());
            assert!(config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("limit of 100 entries"));
        }
    }

    #[test]
    fn reference_rpc_is_required_and_defaults_and_errors_are_safe() {
        let text = example();
        let config = Config::from_yaml(&text).unwrap();
        let reference = &config.slot_validation;
        assert_eq!(reference.slot_buffer, 25);
        assert_eq!(reference.poll_interval, Duration::from_secs(1));
        assert_eq!(reference.request_timeout, Duration::from_secs(2));
        assert_eq!(reference.max_reference_age, Duration::from_secs(5));
        assert_eq!(
            Config::from_yaml(&text.replace("  slot_buffer: 25\n", ""))
                .unwrap()
                .slot_validation
                .slot_buffer,
            25
        );
        for (name, value) in [
            ("poll_interval", "0s"),
            ("request_timeout", "0s"),
            ("max_reference_age", "1s"),
            ("max_reference_age", "2s"),
        ] {
            let invalid = text.replace(
                "  slot_buffer: 25",
                &format!("  slot_buffer: 25\n  {name}: {value}"),
            );
            assert!(Config::from_yaml(&invalid).is_err(), "{name}={value}");
        }
        assert!(Config::from_yaml(&text.replace(
            "BENCH_REFERENCE_RPC_URL",
            "BENCH_TEST_MISSING_REFERENCE_RPC_URL"
        ))
        .is_err());
        std::env::set_var(
            "BENCH_TEST_INVALID_REFERENCE_RPC_URL",
            "file:///private-test-token",
        );
        let invalid = text.replace(
            "BENCH_REFERENCE_RPC_URL",
            "BENCH_TEST_INVALID_REFERENCE_RPC_URL",
        );
        assert!(!format!("{:#}", Config::from_yaml(&invalid).unwrap_err())
            .contains("private-test-token"));
        assert!(
            Config::from_yaml(&text.replace("slot_validation:", "unused_slot_validation:"))
                .is_err()
        );
    }

    #[test]
    fn parses_example_and_validates_limits() {
        let text = example();
        let config = Config::from_yaml(&text).unwrap();
        assert_eq!(config.providers.len(), 2);
        assert_eq!(config.window, Duration::from_secs(5));
        assert_eq!(config.expired_key_retention, Duration::from_secs(120));
        // 2m retention / (5s window + 2s grace + 1s) = 15 spans of 100,000 keys.
        assert_eq!(config.expired_key_capacity(), 1_500_000);
        let explicit = text.replace(
            "expired_key_retention: \"2m\"\n",
            "expired_key_retention: \"2m\"\nexpired_key_capacity: 42\n",
        );
        assert_eq!(
            Config::from_yaml(&explicit).unwrap().expired_key_capacity(),
            42
        );
        assert!(Config::from_yaml(&explicit.replace(": 42", ": 0")).is_err());
        assert_eq!(config.connection_warmup, Duration::from_secs(1));
        assert_eq!(config.slot_confirmation_timeout, Duration::from_secs(30));
        let omitted = text.replace("slot_confirmation_timeout: \"30s\"\n", "");
        assert_ne!(omitted, text);
        assert_eq!(
            Config::from_yaml(&omitted)
                .unwrap()
                .slot_confirmation_timeout,
            Duration::from_secs(30)
        );
        let omitted = text.replace("connection_warmup: \"1s\"\n", "");
        assert_ne!(omitted, text);
        assert_eq!(
            Config::from_yaml(&omitted).unwrap().connection_warmup,
            Duration::from_secs(1)
        );
        assert_eq!(
            Config::from_yaml(
                &text.replace("connection_warmup: \"1s\"", "connection_warmup: \"0s\"")
            )
            .unwrap()
            .connection_warmup,
            Duration::ZERO
        );
        let omitted = text.replace("expired_key_retention: \"2m\"\n", "");
        assert_ne!(omitted, text);
        assert_eq!(
            Config::from_yaml(&omitted).unwrap().expired_key_retention,
            Duration::from_secs(120)
        );
        assert!(Config::from_yaml(&text.replace(
            "expired_key_retention: \"2m\"",
            "expired_key_retention: \"0s\""
        ))
        .is_err());
        assert_eq!(config.filters.accounts_owner.len(), 1);
        assert_eq!(config.filters.transactions_account_include.len(), 1);
        for (field, invalid) in [
            ("batch_size: 50000", "batch_size: 0"),
            ("matcher_key_capacity: 100000", "matcher_key_capacity: 0"),
            ("flush_interval: \"1s\"", "flush_interval: \"0s\""),
            (
                "arrival_buffer_capacity: 10000",
                "arrival_buffer_capacity: 0",
            ),
            (
                "storage_buffer_capacity: 100000",
                "storage_buffer_capacity: 0",
            ),
        ] {
            assert!(Config::from_yaml(&text.replace(field, invalid)).is_err());
        }
    }

    #[test]
    fn removed_late_period_explains_the_single_window_migration() {
        let text = example().replace("window: \"5s\"", "window: \"1s\"\nlate_period: \"10s\"");
        let error = format!("{:#}", Config::from_yaml(&text).unwrap_err());
        assert!(error.contains("`late_period` was removed"), "{error}");
        assert!(error.contains("counts as a missed deadline"), "{error}");
    }

    #[test]
    fn subscriber_requires_region_and_cloud_provider() {
        let text = example();
        let config = Config::from_yaml(&text).unwrap();
        assert_eq!(config.subscriber.region, "example-region");
        assert_eq!(config.subscriber.cloud_provider, "example-cloud");
        let omitted = text.replace("  cloud_provider: \"example-cloud\"\n", "");
        assert_ne!(omitted, text);
        assert!(Config::from_yaml(&omitted).is_err());
        let custom = text.replace(
            "cloud_provider: \"example-cloud\"",
            "cloud_provider: \"AWS\"",
        );
        assert_eq!(
            Config::from_yaml(&custom)
                .unwrap()
                .subscriber
                .cloud_provider,
            "AWS"
        );
        for invalid in ["", "   "] {
            assert!(Config::from_yaml(&text.replace(
                "cloud_provider: \"example-cloud\"",
                &format!("cloud_provider: {invalid:?}")
            ))
            .is_err());
            assert!(Config::from_yaml(&text.replace(
                "region: \"example-region\"",
                &format!("region: {invalid:?}")
            ))
            .is_err());
        }
        assert!(
            Config::from_yaml(&text.replace("subscriber:", "subscriber:\n  id: old-id")).is_err()
        );
    }

    #[test]
    fn subscriber_coordinates_are_required_and_validated() {
        let text = example();
        let mut subscriber = Config::from_yaml(&text).unwrap().subscriber;
        assert_eq!(
            (subscriber.latitude, subscriber.longitude),
            (50.1109, 8.6821)
        );
        let latitude_line = "  latitude: 50.1109\n";
        let longitude_line = "  longitude: 8.6821\n";
        for incomplete in [
            text.replace(latitude_line, ""),
            text.replace(longitude_line, ""),
            text.replace(latitude_line, "").replace(longitude_line, ""),
            text.replace(latitude_line, "  latitude: null\n"),
            text.replace(longitude_line, "  longitude: null\n"),
        ] {
            assert!(Config::from_yaml(&incomplete).is_err());
        }
        for (latitude, longitude) in [
            (91.0, 0.0),
            (0.0, -181.0),
            (f64::NAN, 0.0),
            (0.0, f64::INFINITY),
        ] {
            subscriber.latitude = latitude;
            subscriber.longitude = longitude;
            assert!(subscriber.validate().is_err());
        }
        for (latitude, longitude) in [(-90.0, -180.0), (90.0, 180.0), (0.0, 0.0)] {
            subscriber.latitude = latitude;
            subscriber.longitude = longitude;
            assert!(subscriber.validate().is_ok());
        }
    }

    #[test]
    fn rejects_duplicate_and_missing_providers_and_credentials() {
        let text = example();
        let mut config = Config::from_yaml(&text).unwrap();
        config.providers.pop();
        assert!(config.validate().is_err());
        assert!(
            Config::from_yaml(&text.replace("name: \"provider-b\"", "name: \"provider-a\""))
                .is_err()
        );
        assert!(Config::from_yaml(&text.replace("name: \"provider-b\"", "name: \"\"")).is_err());
        for separator in [";", ",", "\\\"", "=", "@"] {
            let name = format!("name: \"provider{separator}b\"");
            assert!(Config::from_yaml(&text.replace("name: \"provider-b\"", &name)).is_err());
        }
        assert!(Config::from_yaml(
            &text.replace("BENCH_TEST_CH_USER", "BENCH_UNSET_USER_987654321")
        )
        .is_err());
        assert!(
            Config::from_yaml(&text.replace("BENCH_TEST_PROVIDER_TOKEN", "INVALID=TOKEN")).is_err()
        );
        assert!(Config::from_yaml(&text.replace(
            "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
            "not-a-public-key"
        ))
        .is_err());
        assert!(Config::from_yaml(
            &text.replace("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", "111")
        )
        .is_err());
        assert!(Config::from_yaml(
            &text.replace("accounts_owner:", "unknown_filter: true\n  accounts_owner:")
        )
        .is_err());
        assert!(Config::from_yaml("providers: [").is_err());
    }
}
