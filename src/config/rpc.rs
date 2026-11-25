use garde::Validate;
use jsonrpsee::core::JsonValue;
use serde::Deserialize;
use std::collections::HashMap;

use crate::utils::by_name;

#[derive(Clone, Deserialize, Debug, Eq, PartialEq)]
pub struct CacheParams {
    #[serde(default)]
    pub size: Option<usize>,
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
}

#[derive(Clone, Deserialize, Debug, Eq, PartialEq)]
pub struct MethodParam {
    pub name: String,
    #[serde(default)]
    pub ty: String,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub inject: bool,
    /// Other names a by-name (object) call may use for this param, such as the
    /// node's own argument name where it differs from `name`.
    #[serde(default)]
    pub aliases: Vec<String>,
}

#[derive(Deserialize, Validate, Debug)]
#[garde(allow_unvalidated)]
pub struct RpcMethod {
    pub method: String,

    #[serde(default)]
    pub cache: Option<CacheParams>,

    #[garde(custom(validate_params_with_name(&self.method)))]
    #[serde(default)]
    pub params: Vec<MethodParam>,

    #[serde(default)]
    pub response: Option<JsonValue>,

    #[serde(default)]
    pub delay_ms: Option<u64>,

    /// This should not exceed max cell capacity. If it does,
    /// method will return error. Burst size is the max cell capacity.
    /// If rate limit is not configured, this will be ignored.
    /// e.g. if rate limit is configured as 10r per 2s and rate_limit_weight is 10,
    /// then only 1 call is allowed per 2s. If rate_limit_weight is 5, then 2 calls
    /// are allowed per 2s. If rate_limit_weight is greater than 10, then method will
    /// return error "rate limit exceeded".
    /// Add this if you want to modify the default value of 1.
    #[serde(default = "default_rate_limit_weight")]
    pub rate_limit_weight: u32,
}

fn validate_params_with_name(method_name: &str) -> impl FnOnce(&[MethodParam], &()) -> garde::Result + '_ {
    move |params, _| {
        // ensure each method has only one param with inject=true
        if params.iter().filter(|x| x.inject).count() > 1 {
            return Err(garde::Error::new(format!(
                "method {method_name} has more than one inject param"
            )));
        }
        validate_param_names(method_name, params)?;
        // ensure there is no required param after optional param
        let mut has_optional = false;
        for param in params {
            if param.optional {
                has_optional = true;
            } else if has_optional {
                return Err(garde::Error::new(format!(
                    "method {method_name} has required param after optional param"
                )));
            }
        }
        Ok(())
    }
}

/// By-name (object) params are mapped to positions by key, so every key a
/// caller may use must lead to exactly one param: no name or alias may be
/// empty, and no key form (the name, each alias, and the snake_case and
/// lowerCamelCase forms of each, see `utils::by_name`) may belong to two
/// params of one method. Otherwise `{"at": ..}` could silently land in
/// another param's position.
fn validate_param_names(method_name: &str, params: &[MethodParam]) -> garde::Result {
    // key form -> (param index, the name or alias it comes from)
    let mut seen: HashMap<String, (usize, &str)> = HashMap::new();
    for (idx, param) in params.iter().enumerate() {
        if param.name.trim().is_empty() {
            return Err(garde::Error::new(format!(
                "method {method_name} has a param with an empty name (param {idx})"
            )));
        }
        for name in std::iter::once(&param.name).chain(param.aliases.iter()) {
            if name.trim().is_empty() {
                return Err(garde::Error::new(format!(
                    "method {method_name} param \"{}\" has an empty alias",
                    param.name
                )));
            }
            for form in by_name::key_forms(name) {
                match seen.get(&form) {
                    Some(&(other, other_name)) if other != idx => {
                        return Err(garde::Error::new(format!(
                            "method {method_name} has ambiguous param names: \"{form}\" names both \"{}\" (param {other}, from \"{other_name}\") and \"{}\" (param {idx}, from \"{name}\")",
                            params[other].name, param.name
                        )));
                    }
                    Some(_) => {}
                    None => {
                        seen.insert(form, (idx, name.as_str()));
                    }
                }
            }
        }
    }
    Ok(())
}

fn default_rate_limit_weight() -> u32 {
    1
}

#[derive(Copy, Clone, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    // Replace old value with new value
    Replace,
    // Merge old storage changes with new changes
    MergeStorageChanges,
}

#[derive(Deserialize, Validate, Debug)]
#[garde(allow_unvalidated)]
pub struct RpcSubscription {
    pub subscribe: String,
    pub unsubscribe: String,
    pub name: String,

    #[serde(default)]
    pub merge_strategy: Option<MergeStrategy>,

    /// The subscribe call's params. Only used to convert by-name (object)
    /// params to positional ones; positional params are forwarded as given.
    #[garde(custom(validate_params_with_name(&self.subscribe)))]
    #[serde(default)]
    pub params: Vec<MethodParam>,
}

#[derive(Deserialize, Validate, Debug)]
#[garde(allow_unvalidated)]
pub struct RpcDefinitions {
    #[garde(dive)]
    pub methods: Vec<RpcMethod>,
    #[garde(dive)]
    #[serde(default)]
    pub subscriptions: Vec<RpcSubscription>,
    #[serde(default)]
    pub aliases: Vec<(String, String)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_params_succeeds_for_valid_params() {
        let valid_params = vec![
            MethodParam {
                name: "param1".to_string(),
                ty: "u64".to_string(),
                optional: false,
                inject: false,
                aliases: vec![],
            },
            MethodParam {
                name: "param2".to_string(),
                ty: "u64".to_string(),
                optional: true,
                inject: false,
                aliases: vec![],
            },
            MethodParam {
                name: "param3".to_string(),
                ty: "u64".to_string(),
                optional: true,
                inject: false,
                aliases: vec![],
            },
        ];
        let method_name = "test";
        let test_fn = validate_params_with_name(method_name);
        assert!(test_fn(&valid_params, &()).is_ok());
    }

    #[test]
    fn validate_params_fails_for_more_than_one_param_has_inject_equals_true() {
        let another_invalid_params = vec![
            MethodParam {
                name: "param1".to_string(),
                ty: "u64".to_string(),
                optional: false,
                inject: true,
                aliases: vec![],
            },
            MethodParam {
                name: "param2".to_string(),
                ty: "u64".to_string(),
                optional: false,
                inject: true,
                aliases: vec![],
            },
            MethodParam {
                name: "param3".to_string(),
                ty: "u64".to_string(),
                optional: false,
                inject: true,
                aliases: vec![],
            },
        ];
        let method_name = "test";
        let test_fn = validate_params_with_name(method_name);
        assert!(test_fn(&another_invalid_params, &()).is_err());
    }

    #[test]
    fn validate_params_fails_for_optional_params_are_not_the_last() {
        let method_name = "test";
        let invalid_params = vec![
            MethodParam {
                name: "param1".to_string(),
                ty: "u64".to_string(),
                optional: false,
                inject: false,
                aliases: vec![],
            },
            MethodParam {
                name: "param2".to_string(),
                ty: "u64".to_string(),
                optional: true,
                inject: false,
                aliases: vec![],
            },
            MethodParam {
                name: "param3".to_string(),
                ty: "u64".to_string(),
                optional: false,
                inject: true,
                aliases: vec![],
            },
        ];
        let test_fn = validate_params_with_name(method_name);
        assert!(test_fn(&invalid_params, &()).is_err());
    }

    fn named(name: &str, aliases: &[&str]) -> MethodParam {
        MethodParam {
            name: name.to_string(),
            ty: String::new(),
            optional: false,
            inject: false,
            aliases: aliases.iter().map(|a| a.to_string()).collect(),
        }
    }

    fn name_error(params: &[MethodParam]) -> String {
        validate_params_with_name("test")(params, &()).unwrap_err().to_string()
    }

    #[test]
    fn validate_params_fails_for_duplicate_name() {
        let err = name_error(&[named("key", &[]), named("key", &[])]);
        assert!(err.contains("ambiguous param names"), "{err}");
    }

    #[test]
    fn validate_params_fails_for_alias_equal_to_another_params_name() {
        // `{"at": <storage key>}` would be taken as `key`
        let err = name_error(&[named("key", &["at"]), named("at", &[])]);
        assert!(err.contains("\"at\" names both \"key\""), "{err}");
        // whichever param comes first
        let err = name_error(&[named("key", &[]), named("at", &["key"])]);
        assert!(err.contains("ambiguous param names"), "{err}");
    }

    #[test]
    fn validate_params_fails_for_alias_equal_to_another_params_alias() {
        let err = name_error(&[named("key", &["hash"]), named("at", &["hash"])]);
        assert!(err.contains("ambiguous param names"), "{err}");
    }

    #[test]
    fn validate_params_fails_for_names_colliding_only_by_case_form() {
        // neither name equals the other, but both accept `start_key` and `startKey`
        let err = name_error(&[named("startKey", &[]), named("start_key", &[])]);
        assert!(err.contains("ambiguous param names"), "{err}");
        // an alias's form against a name
        let err = name_error(&[named("x", &["BlockHash"]), named("block_hash", &[])]);
        assert!(err.contains("ambiguous param names"), "{err}");
    }

    #[test]
    fn validate_params_fails_for_empty_alias() {
        let err = name_error(&[named("key", &[""])]);
        assert!(err.contains("empty alias"), "{err}");
    }

    #[test]
    fn validate_params_fails_for_empty_name() {
        let err = name_error(&[named("", &[])]);
        assert!(err.contains("empty name"), "{err}");
        let err = name_error(&[named("key", &[]), named(" ", &["at"])]);
        assert!(err.contains("empty name"), "{err}");
    }

    #[test]
    fn validate_params_allows_a_param_to_repeat_its_own_forms() {
        // `startKey` already accepts `start_key`; saying so is not ambiguous
        let params = [named("key", &["prefix"]), named("startKey", &["start_key", "startKey"])];
        assert!(validate_params_with_name("test")(&params, &()).is_ok());
    }

    fn definitions(yaml: &str) -> RpcDefinitions {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn subscription_params_are_validated() {
        let ok = definitions(
            r#"
methods: []
subscriptions:
  - subscribe: state_subscribeStorage
    unsubscribe: state_unsubscribeStorage
    name: state_storage
    params:
      - name: keys
        optional: true
"#,
        );
        assert!(ok.validate(&()).is_ok());

        for params in [
            "[{name: keys, aliases: [at]}, {name: at}]",
            "[{name: keys}, {name: keys}]",
            "[{name: keys, aliases: ['']}]",
            "[{name: ''}]",
            "[{name: a, optional: true}, {name: b}]",
        ] {
            let bad = definitions(&format!(
                "methods: []\nsubscriptions:\n  - subscribe: s\n    unsubscribe: u\n    name: n\n    params: {params}\n"
            ));
            assert!(bad.validate(&()).is_err(), "{params} passed validation");
        }
    }

    #[test]
    fn method_params_are_validated_through_definitions() {
        // the state_getStorage config from the review: `{"at": <key>}` mapped to `key`
        let bad = definitions(
            r#"
methods:
  - method: state_getStorage
    params:
      - name: key
        aliases: [ at ]
      - name: at
        optional: true
"#,
        );
        let err = bad.validate(&()).unwrap_err().to_string();
        assert!(err.contains("state_getStorage has ambiguous param names"), "{err}");
    }

    #[test]
    fn every_shipped_config_validates() {
        use crate::config::{ETHEREUM_CONFIG, SUBSTRATE_CONFIG};
        for (label, src) in [("substrate.yml", SUBSTRATE_CONFIG), ("ethereum.yml", ETHEREUM_CONFIG)] {
            let defs = definitions(src);
            assert!(!defs.methods.is_empty(), "{label}");
            defs.validate(&()).unwrap_or_else(|e| panic!("{label}: {e}"));
        }
        for path in [
            "configs/config.yml",
            "configs/eth_config.yml",
            "tests/configs/config_with_env.yml",
            "tests/configs/broken_endpoints.yml",
            "tests/configs/big_rate_limit_weight.yml",
        ] {
            let config = crate::config::read_config(path).unwrap_or_else(|e| panic!("{path}: {e}"));
            config.validate(&()).unwrap_or_else(|e| panic!("{path}: {e}"));
        }
    }
}
