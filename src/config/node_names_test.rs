//! Pins the configured param names against the node's argument names.
//!
//! By-name callers learn param names from the node, so every configured
//! method's params must accept, position by position, the node's argument
//! name (by `name`, an alias, or the snake_case/camelCase form of either).
//! There is no node in CI: tests/node_param_names.json is the reference,
//! extracted from the node's source by scripts/extract_node_param_names.py.
//! When it is regenerated for a new node revision, a renamed, added or moved
//! argument fails here instead of at a caller.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::{MethodParam, RpcDefinitions, ETHEREUM_CONFIG, SUBSTRATE_CONFIG};
use crate::utils::by_name::accepts;

const NODE_NAMES: &str = include_str!("../../tests/node_param_names.json");

#[derive(Deserialize)]
struct NodeNames {
    methods: BTreeMap<String, Vec<String>>,
}

fn node_names() -> BTreeMap<String, Vec<String>> {
    serde_json::from_str::<NodeNames>(NODE_NAMES).unwrap().methods
}

/// Mismatches between one method's configured params and the node's names.
/// A trailing "?" on a node name marks an argument the node lets a caller
/// leave out; a param configured optional must be one of those, since a gap
/// before the last supplied param is filled with null.
fn mismatches(method: &str, params: &[MethodParam], node: &[String]) -> Vec<String> {
    let mut out = vec![];
    if params.len() != node.len() {
        out.push(format!(
            "{method}: {} params configured, the node takes {} ({node:?})",
            params.len(),
            node.len()
        ));
    }
    for (i, (param, node_name)) in params.iter().zip(node).enumerate() {
        let (name, node_optional) = match node_name.strip_suffix('?') {
            Some(name) => (name, true),
            None => (node_name.as_str(), false),
        };
        if !accepts(param, name) {
            out.push(format!(
                "{method}: param {i} \"{}\" does not accept the node's name \"{name}\" (add it to aliases)",
                param.name
            ));
        }
        if param.optional && !node_optional {
            out.push(format!(
                "{method}: param {i} \"{}\" is optional but the node requires \"{name}\"",
                param.name
            ));
        }
    }
    out
}

fn check(label: &str, src: &str) -> (usize, Vec<String>) {
    let defs: RpcDefinitions = serde_yaml::from_str(src).unwrap();
    let node = node_names();
    let mut checked = 0;
    let mut out = vec![];
    let methods = defs.methods.iter().map(|m| (&m.method, &m.params));
    let subscriptions = defs.subscriptions.iter().map(|s| (&s.subscribe, &s.params));
    for (method, params) in methods.chain(subscriptions) {
        // methods the node does not define (Subway-only or other chains')
        let Some(names) = node.get(method) else { continue };
        checked += 1;
        out.extend(
            mismatches(method, params, names)
                .into_iter()
                .map(|m| format!("{label}: {m}")),
        );
    }
    (checked, out)
}

#[test]
fn substrate_params_cover_node_arg_names() {
    let (checked, errors) = check("substrate.yml", SUBSTRATE_CONFIG);
    assert!(checked >= 20, "only {checked} methods checked: is the fixture loaded?");
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

#[test]
fn ethereum_params_cover_node_arg_names() {
    let (checked, errors) = check("ethereum.yml", ETHEREUM_CONFIG);
    assert!(checked >= 20, "only {checked} methods checked: is the fixture loaded?");
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

#[test]
fn fixture_has_the_known_names() {
    let node = node_names();
    assert_eq!(node["chain_getBlockHash"], ["hash?"]);
    assert_eq!(node["state_call"], ["name", "bytes", "hash?"]);
    assert_eq!(node["eth_getTransactionCount"], ["address", "number_or_hash?"]);
}

#[test]
fn a_renamed_node_argument_is_reported() {
    let params = vec![MethodParam {
        name: "blockNumber".to_string(),
        ty: String::new(),
        optional: true,
        inject: true,
        aliases: vec![],
    }];
    let errors = mismatches("chain_getBlockHash", &params, &["hash?".to_string()]);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(mismatches("chain_getBlockHash", &params, &["block_number?".to_string()]).is_empty());
    assert_eq!(mismatches("m", &params, &[]).len(), 1);
}
