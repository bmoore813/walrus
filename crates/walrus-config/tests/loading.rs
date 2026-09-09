//! Integration coverage for the shared YAML loader's public service projections.

use walrus_config::{ConfigError, ExtractorConfig, TransformerConfig};

const VALID_YAML: &str = r#"
version: 1
common:
  control_db_url: postgres://control/db
  object_store:
    bucket: staging
extractor:
  source_db_url: postgres://source/db
  instance: extractor-0
  slot_name: walrus_slot
  publication_name: walrus_pub
transformer:
  instance: transformer-0
  ducklake:
    catalog_url: postgres://catalog/db
    data_path: s3://staging/ducklake/
"#;

#[allow(
    clippy::result_large_err,
    reason = "figment Jail requires Result<(), figment::Error>, whose error variant is intentionally large"
)]
fn in_jail(body: impl FnOnce(&mut figment::Jail)) {
    figment::Jail::expect_with(|jail| {
        body(jail);
        Ok(())
    });
}

#[test]
fn both_services_load_the_same_document() {
    in_jail(|jail| {
        jail.create_file("walrus.yaml", VALID_YAML)
            .expect("create shared config fixture");
        let extractor = ExtractorConfig::load_from("walrus.yaml")
            .expect("extractor should load the shared fixture");
        let transformer = TransformerConfig::load_from("walrus.yaml")
            .expect("transformer should load the shared fixture");

        assert_eq!(extractor.common.object_store.bucket, "staging");
        assert_eq!(transformer.common.object_store.bucket, "staging");
        assert_eq!(extractor.extractor.slot_name, "walrus_slot");
        assert_eq!(transformer.transformer.ducklake.attach_name, "walrus");
    });
}

#[test]
fn flat_environment_overrides_route_to_common_and_the_active_service() {
    in_jail(|jail| {
        jail.create_file("walrus.yaml", VALID_YAML)
            .expect("create shared config fixture");
        jail.set_env("WALRUS_OBJECT_STORE__BUCKET", "override-bucket");
        jail.set_env("WALRUS_SLOT_NAME", "override_slot");

        let config = ExtractorConfig::load_from("walrus.yaml")
            .expect("valid environment overrides should load");
        assert_eq!(config.common.object_store.bucket, "override-bucket");
        assert_eq!(config.extractor.slot_name, "override_slot");
    });
}

#[test]
fn inactive_service_semantics_do_not_block_the_active_service() {
    in_jail(|jail| {
        let yaml = VALID_YAML.replace(
            "transformer:\n  instance: transformer-0\n  ducklake:\n    catalog_url: postgres://catalog/db\n    data_path: s3://staging/ducklake/",
            "transformer: {}",
        );
        jail.create_file("walrus.yaml", &yaml)
            .expect("create shared config fixture");

        assert!(ExtractorConfig::load_from("walrus.yaml").is_ok());
        assert!(TransformerConfig::load_from("walrus.yaml").is_err());
    });
}

#[test]
fn unknown_keys_anywhere_in_the_document_are_rejected() {
    in_jail(|jail| {
        let yaml = VALID_YAML.replace("transformer:\n", "transformer:\n  poll_intervl: 1s\n");
        jail.create_file("walrus.yaml", &yaml)
            .expect("create shared config fixture");

        assert!(matches!(
            ExtractorConfig::load_from("walrus.yaml"),
            Err(ConfigError::Load(_))
        ));
    });
}

#[test]
fn active_service_rejects_unknown_environment_overrides() {
    in_jail(|jail| {
        jail.create_file("walrus.yaml", VALID_YAML)
            .expect("create shared config fixture");
        jail.set_env("WALRUS_MAX_ROS", "10");

        assert!(matches!(
            ExtractorConfig::load_from("walrus.yaml"),
            Err(ConfigError::Load(_))
        ));
    });
}

#[test]
fn unsupported_versions_are_explicit() {
    in_jail(|jail| {
        jail.create_file(
            "walrus.yaml",
            &VALID_YAML.replace("version: 1", "version: 2"),
        )
        .expect("create shared config fixture");

        assert!(matches!(
            ExtractorConfig::load_from("walrus.yaml"),
            Err(ConfigError::UnsupportedVersion {
                configured: 2,
                supported: 1
            })
        ));
    });
}

#[test]
fn normal_load_requires_walrus_config() {
    in_jail(|_| {
        assert!(matches!(
            ExtractorConfig::load(),
            Err(ConfigError::MissingConfigPath)
        ));
    });
}

#[test]
fn toml_paths_are_rejected() {
    in_jail(|_| {
        assert!(matches!(
            TransformerConfig::load_from("walrus.toml"),
            Err(ConfigError::UnsupportedFormat(_))
        ));
    });
}
