//! YAML and environment-provider composition.

use crate::{CONFIG_VERSION, CommonConfig, ConfigError, ExtractorSettings, TransformerSettings};
use figment::Figment;
use figment::providers::{Env, Format, Yaml};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Service {
    Extractor,
    Transformer,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigDocument {
    pub(crate) version: u64,
    pub(crate) common: CommonConfig,
    pub(crate) extractor: ExtractorSettings,
    pub(crate) transformer: TransformerSettings,
}

pub(crate) fn load_from_environment(service: Service) -> Result<ConfigDocument, ConfigError> {
    let path = std::env::var_os("WALRUS_CONFIG").ok_or(ConfigError::MissingConfigPath)?;
    load(Path::new(&path), service)
}

pub(crate) fn load(path: &Path, service: Service) -> Result<ConfigDocument, ConfigError> {
    let extension = path.extension().and_then(|value| value.to_str());
    if !matches!(extension, Some("yaml" | "yml")) {
        return Err(ConfigError::UnsupportedFormat(path.to_path_buf()));
    }

    let active_section = match service {
        Service::Extractor => "extractor",
        Service::Transformer => "transformer",
    };
    let environment = Env::prefixed("WALRUS_")
        .ignore(&["config", "CONFIG"])
        .map(move |key| {
            if key == "control_db_url"
                || key.starts_with("object_store__")
                || key.starts_with("telemetry__")
            {
                format!("common__{}", key.as_str()).into()
            } else {
                format!("{active_section}__{}", key.as_str()).into()
            }
        })
        .split("__");

    let document: ConfigDocument = Figment::new()
        .merge(Yaml::file(path))
        .merge(environment)
        .extract()
        .map_err(|source| ConfigError::Load(Box::new(source)))?;
    if document.version != CONFIG_VERSION {
        return Err(ConfigError::UnsupportedVersion {
            configured: document.version,
            supported: CONFIG_VERSION,
        });
    }
    Ok(document)
}
