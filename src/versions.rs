//! Subscription versions: the stored identity of each stream's measurement
//! configuration. Dashboards never mix results across versions.
use std::time::Duration;

use prost::Message;

use crate::{
    config::{FiltersConfig, ProviderConfig, SlotValidationConfig},
    model::{DataType, StreamKind},
    storage::SubscriptionVersion,
    stream,
};

/// The ID covers everything that defines a measurement's meaning: the normalized
/// request, the on-time window and the compared providers. "Lag behind ideal" and
/// win shares are relative to every configured provider, so adding, removing,
/// renaming or re-pointing one starts a new configuration, and dashboards never mix
/// results measured under different definitions. Provider order and tokens are not
/// part of it (a rotated credential is the same measurement). Endpoint URLs can
/// embed credentials, so the stored description shows only a short hash of each.
/// The RPC source and validation policy also define which events can be measured.
pub fn subscription_versions(
    filters: &FiltersConfig,
    window: Duration,
    providers: &[ProviderConfig],
    validation: &SlotValidationConfig,
    rpc_url: &str,
) -> [SubscriptionVersion; StreamKind::ALL.len()] {
    let filters = filters.normalized();
    let mut providers: Vec<(&str, &str)> = providers
        .iter()
        .map(|provider| {
            let url = provider.url.trim().trim_end_matches('/');
            (provider.name.as_str(), url)
        })
        .collect();
    providers.sort_unstable();
    let provider_set = providers
        .iter()
        .map(|(name, url)| format!("{name}@{}", &blake3::hash(url.as_bytes()).to_hex()[..8]))
        .collect::<Vec<_>>()
        .join(",");
    std::array::from_fn(|i| {
        let kind = StreamKind::ALL[i];
        // Hash the actual normalized protobuf request, never provider credentials.
        let request = stream::subscribe_request(kind, &filters);
        let mut hash = blake3::Hasher::new();
        hash.update(b"yellowstone-subscription-v3-rpc-slots\0");
        if kind.data_type == DataType::Account {
            hash.update(b"account-key-v2:slot,pubkey,txn_signature\0");
        }
        hash.update(&validation.slot_buffer.to_be_bytes());
        for duration in [
            validation.poll_interval,
            validation.request_timeout,
            validation.max_reference_age,
        ] {
            hash.update(&duration.as_nanos().to_be_bytes());
        }
        let rpc_hash = blake3::hash(rpc_url.trim().trim_end_matches('/').as_bytes());
        hash.update(rpc_hash.as_bytes());
        hash.update(&request.encode_to_vec());
        hash.update(b"\0window-ns\0");
        hash.update(&window.as_nanos().to_be_bytes());
        hash.update(b"\0providers\0");
        for field in providers.iter().flat_map(|(name, url)| [name, url]) {
            hash.update(&(field.len() as u64).to_be_bytes());
            hash.update(field.as_bytes());
        }
        let description = match kind.data_type {
            DataType::Account => format!(
                "accounts.owner={:?};nonempty_txn_signature=true;account_key=slot,pubkey,txn_signature",
                filters.accounts_owner
            ),
            DataType::Transaction => format!(
                "transactions.account_include={:?};vote=all;failed={}",
                filters.transactions_account_include,
                match filters.transactions_failed {
                    None => "all",
                    Some(true) => "true",
                    Some(false) => "false",
                }
            ),
            DataType::Slot => "slots.filter_by_commitment=false;interslot_updates=true".into(),
        };
        SubscriptionVersion {
            id: hash.finalize().to_hex().to_string(),
            filters: format!(
                "{};commitment={};window={};providers={provider_set};slot_buffer={};reference_rpc={};{description}",
                kind.data_type.as_str(),
                kind.commitment.as_str(),
                format_window(window), validation.slot_buffer, &rpc_hash.to_hex()[..8]
            ),
        }
    })
}

/// Compact, exact rendering for configuration labels, e.g. "5s" or "1500ms".
fn format_window(window: Duration) -> String {
    if window.subsec_nanos() == 0 {
        format!("{}s", window.as_secs())
    } else if window.subsec_nanos().is_multiple_of(1_000_000) {
        format!("{}ms", window.as_millis())
    } else {
        format!("{}ns", window.as_nanos())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn transaction_aware_accounts_start_new_histories_without_changing_other_streams() {
        let config = Config::example();
        let versions = subscription_versions(
            &config.filters,
            config.window,
            &config.providers,
            &config.slot_validation,
            &config.slot_validation.rpc_url().unwrap(),
        );
        // IDs captured before the account-key change, with the example config
        // and the same synthetic reference URL used by Config::example().
        let previous = [
            "c0ab45f32d6aef00fdf0f5d586843032d6c7378890a783121b23731cbba50308",
            "ea1538592872de905b063720227d0749d9861b6ee809ee52c82b6f7776c0a5de",
            "878df80495dd88093a3430c174a9bd6b9d90329ee18f208b3056e01ec69dc0eb",
            "1ae3e4a8c57822737a9359fff92f40158b29f98f2f2253bc5430ec615e71dcae",
            "cb6b7b069b92000631fd82e2722eccf4ddc88879ba7515549842717dcb4ed45b",
        ];
        for (index, version) in versions.iter().enumerate() {
            if index < 2 {
                assert_ne!(version.id, previous[index]);
                assert!(version.filters.contains(
                    ";nonempty_txn_signature=true;account_key=slot,pubkey,txn_signature"
                ));
            } else {
                assert_eq!(version.id, previous[index]);
            }
        }
    }

    #[test]
    fn failed_filter_starts_new_transaction_histories_only() {
        let config = Config::example();
        let url = config.slot_validation.rpc_url().unwrap();
        let versions = |filters: &FiltersConfig| {
            subscription_versions(
                filters,
                config.window,
                &config.providers,
                &config.slot_validation,
                &url,
            )
        };
        let all = versions(&config.filters);
        for (failed, label) in [(Some(false), "false"), (Some(true), "true")] {
            let mut filters = config.filters.clone();
            filters.transactions_failed = failed;
            for (index, (a, b)) in all.iter().zip(versions(&filters)).enumerate() {
                if StreamKind::ALL[index].data_type == DataType::Transaction {
                    assert!(a.filters.ends_with(";vote=all;failed=all"));
                    assert!(b.filters.ends_with(&format!(";vote=all;failed={label}")));
                    assert_ne!(a.id, b.id);
                } else {
                    assert_eq!((&a.id, &a.filters), (&b.id, &b.filters));
                }
            }
        }
    }

    #[test]
    fn rpc_policy_and_source_change_identity_without_exposing_credentials() {
        let config = Config::example();
        let url = "https://reference.invalid/private-test-token";
        let versions = |validation: &SlotValidationConfig, url| {
            subscription_versions(
                &config.filters,
                config.window,
                &config.providers,
                validation,
                url,
            )
        };
        let a = versions(&config.slot_validation, url);
        let other = versions(
            &config.slot_validation,
            "https://other.invalid/private-test-token",
        );
        for (a, b) in a.iter().zip(other) {
            assert!(a.filters.contains("slot_buffer=25;reference_rpc="));
            assert!(!a.filters.contains("private-test-token"));
            assert!(!a.filters.contains("reference.invalid"));
            assert_ne!(a.id, b.id);
        }
        for field in 0..4 {
            let mut validation = config.slot_validation.clone();
            match field {
                0 => validation.slot_buffer += 1,
                1 => validation.poll_interval += Duration::from_millis(1),
                2 => validation.request_timeout += Duration::from_millis(1),
                _ => validation.max_reference_age += Duration::from_millis(1),
            }
            assert!(a
                .iter()
                .zip(versions(&validation, url))
                .all(|(a, b)| a.id != b.id));
        }
    }

    #[test]
    fn normalized_ids_are_stable_and_distinct_without_credentials() {
        let config = Config::example();
        let filters = config.filters.clone();
        let providers = config.providers.clone();
        let window = Duration::from_secs(5);
        let versions = |filters: &FiltersConfig| {
            subscription_versions(
                filters,
                window,
                &providers,
                &config.slot_validation,
                &config.slot_validation.rpc_url().unwrap(),
            )
        };
        let a = versions(&filters);
        assert_eq!(a[0].id, versions(&filters)[0].id);
        assert!(a.iter().all(|v| v.filters.contains(";window=5s;")));
        // A different on-time window is a different measurement for every stream.
        let other_window = subscription_versions(
            &filters,
            Duration::from_millis(1500),
            &providers,
            &config.slot_validation,
            &config.slot_validation.rpc_url().unwrap(),
        );
        assert!(a.iter().zip(&other_window).all(|(a, b)| a.id != b.id));
        assert!(other_window[0].filters.contains(";window=1500ms;"));
        // The provider set is part of every stream's identity; order and tokens are not.
        let with = |providers: &[ProviderConfig]| {
            subscription_versions(
                &filters,
                window,
                providers,
                &config.slot_validation,
                &config.slot_validation.rpc_url().unwrap(),
            )
        };
        let mut reordered_providers = providers.clone();
        reordered_providers.reverse();
        reordered_providers[0].token_env = Some("ROTATED_TOKEN".into());
        reordered_providers[1].url.push('/');
        assert_eq!(
            a.clone().map(|v| v.id),
            with(&reordered_providers).map(|v| v.id)
        );
        let mut repointed = providers.clone();
        repointed[1].url = "https://provider-b-eu.invalid:443".into();
        let mut renamed = providers.clone();
        renamed[1].name = "provider-c".into();
        let mut added = providers.clone();
        added.push(ProviderConfig {
            name: "provider-c".into(),
            url: "https://provider-c.invalid:443".into(),
            token_env: None,
        });
        for changed in [repointed, renamed, added] {
            assert!(a.iter().zip(&with(&changed)).all(|(a, b)| a.id != b.id));
        }
        // Names are shown; endpoints only as a short hash (URLs may hold credentials).
        let description = &a[0].filters;
        assert!(
            description.contains(";providers=provider-a@"),
            "{description}"
        );
        assert!(description.contains(",provider-b@"), "{description}");
        assert!(!description.contains("invalid"), "{description}");
        assert_eq!(
            a.iter()
                .map(|v| &v.id)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            StreamKind::ALL.len()
        );
        assert!(!a[0].filters.contains("token"));

        let mut reordered = filters.clone();
        // Safety opt-ins do not alter the request while lists are nonempty.
        reordered.allow_all_accounts = true;
        reordered.allow_all_transactions = true;
        reordered
            .accounts_owner
            .extend(filters.accounts_owner.clone());
        reordered
            .transactions_account_include
            .extend(filters.transactions_account_include.clone());
        reordered.accounts_owner.reverse();
        reordered.transactions_account_include.reverse();
        assert_eq!(a.map(|v| v.id), versions(&reordered).map(|v| v.id));

        let mut changed = filters;
        changed
            .accounts_owner
            .push("11111111111111111111111111111111".into());
        let b = versions(&changed);
        assert_ne!(b[0].id, versions(&reordered)[0].id);
        assert_ne!(b[1].id, versions(&reordered)[1].id);
        assert_eq!(b[2].id, versions(&reordered)[2].id);
        assert_eq!(b[4].id, versions(&reordered)[4].id);
    }
}
