//! JSON-RPC 2.0 by-name (object) params.
//!
//! The middleware chain (inject_params, cache, block_tag, upstream) works on
//! positional params, so an object is converted to a positional list using the
//! method's configured params. A key matches a param when it equals the
//! param's name or one of its `aliases`, or the snake_case or lowerCamelCase
//! form of either: the same forms a jsonrpsee node accepts for its own argument
//! names (jsonrpsee-proc-macros 0.24, `decode_map`). Aliases carry the node's
//! argument names where they differ from the config's (`hash` for
//! chain_getBlockHash's `blockNumber`), since by-name callers learn the names
//! from the node.

use jsonrpsee::{core::JsonValue, types::ErrorObjectOwned};

use crate::{config::MethodParam, utils::errors};

/// Converts by-name params to positional ones for `method`, whose configured
/// params are `params`.
///
/// Positions after the last supplied param are left out (an omitted trailing
/// param is the same as a shorter positional list, so inject_params still
/// fills `at`); a gap before it is filled with null when that param is
/// optional and refused otherwise. `{}` is an empty list.
pub fn named_to_positional(
    method: &str,
    params: &[MethodParam],
    named: &serde_json::Map<String, JsonValue>,
) -> Result<Vec<JsonValue>, ErrorObjectOwned> {
    let mut slots: Vec<Option<(&str, JsonValue)>> = vec![None; params.len()];
    for (key, value) in named {
        let Some(idx) = params.iter().position(|p| accepts(p, key)) else {
            return Err(errors::invalid_params(unknown_key_message(method, params, key)));
        };
        if let Some((first, _)) = &slots[idx] {
            return Err(errors::invalid_params(format!(
                "{method}: \"{}\" and \"{}\" both name param \"{}\"",
                echo_key(first),
                echo_key(key),
                params[idx].name
            )));
        }
        slots[idx] = Some((key.as_str(), value.clone()));
    }

    let Some(last) = slots.iter().rposition(Option::is_some) else {
        return Ok(vec![]);
    };
    let mut out = Vec::with_capacity(last + 1);
    for (param, slot) in params.iter().zip(slots).take(last + 1) {
        match slot {
            Some((_, value)) => out.push(value),
            None if param.optional => out.push(JsonValue::Null),
            None => {
                return Err(errors::invalid_params(format!(
                    "{method}: missing required param \"{}\"",
                    param.name
                )))
            }
        }
    }
    Ok(out)
}

pub(crate) fn accepts(param: &MethodParam, key: &str) -> bool {
    std::iter::once(&param.name)
        .chain(param.aliases.iter())
        .any(|name| key_forms(name).iter().any(|form| key == form))
}

/// The keys a by-name call may use for a param named (or aliased) `name`:
/// the name itself and its snake_case and lowerCamelCase forms. Config
/// validation uses the same list, so no key can resolve to two params.
pub(crate) fn key_forms(name: &str) -> [String; 3] {
    [name.to_string(), snake_case(name), lower_camel_case(name)]
}

/// Longest key echoed back in an error. Keys are caller-supplied and can be
/// as large as the request, so an error must not repeat one in full.
const MAX_ECHOED_KEY_CHARS: usize = 64;

fn echo_key(key: &str) -> String {
    match key.char_indices().nth(MAX_ECHOED_KEY_CHARS) {
        Some((end, _)) => format!("{}...", &key[..end]),
        None => key.to_string(),
    }
}

fn unknown_key_message(method: &str, params: &[MethodParam], key: &str) -> String {
    let key = echo_key(key);
    if params.is_empty() {
        return format!("{method}: unknown param \"{key}\"; {method} takes no params");
    }
    let accepted = params
        .iter()
        .map(|p| {
            if p.aliases.is_empty() {
                p.name.clone()
            } else {
                format!("{} (or {})", p.name, p.aliases.join(", "))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{method}: unknown param \"{key}\"; accepted names, in order: {accepted} (snake_case and camelCase forms are accepted too)")
}

/// Splits a name into lowercase words at `_`, `-` and lower-to-upper case
/// boundaries: "blockNumber", "block_number" and "BlockNumber" all give
/// ["block", "number"].
fn words(name: &str) -> Vec<String> {
    let mut words = vec![];
    let mut cur = String::new();
    let mut prev_lower = false;
    for c in name.chars() {
        if c == '_' || c == '-' {
            if !cur.is_empty() {
                words.push(std::mem::take(&mut cur));
            }
            prev_lower = false;
            continue;
        }
        if c.is_uppercase() && prev_lower && !cur.is_empty() {
            words.push(std::mem::take(&mut cur));
        }
        prev_lower = c.is_lowercase() || c.is_ascii_digit();
        cur.extend(c.to_lowercase());
    }
    if !cur.is_empty() {
        words.push(cur);
    }
    words
}

fn snake_case(name: &str) -> String {
    words(name).join("_")
}

fn lower_camel_case(name: &str) -> String {
    let mut out = String::new();
    for (i, w) in words(name).into_iter().enumerate() {
        if i == 0 {
            out.push_str(&w);
        } else {
            let mut chars = w.chars();
            if let Some(first) = chars.next() {
                out.extend(first.to_uppercase());
                out.push_str(chars.as_str());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn param(name: &str, optional: bool, aliases: &[&str]) -> MethodParam {
        MethodParam {
            name: name.to_string(),
            ty: String::new(),
            optional,
            inject: false,
            aliases: aliases.iter().map(|a| a.to_string()).collect(),
        }
    }

    fn convert(params: &[MethodParam], named: JsonValue) -> Result<Vec<JsonValue>, ErrorObjectOwned> {
        named_to_positional("m", params, named.as_object().unwrap())
    }

    fn block_hash() -> Vec<MethodParam> {
        vec![param("blockNumber", true, &["hash"])]
    }

    fn state_call() -> Vec<MethodParam> {
        vec![
            param("method", false, &["name"]),
            param("data", false, &["bytes"]),
            param("at", true, &["hash"]),
        ]
    }

    #[test]
    fn empty_object_is_empty_list() {
        assert_eq!(convert(&block_hash(), json!({})).unwrap(), Vec::<JsonValue>::new());
        assert_eq!(convert(&[], json!({})).unwrap(), Vec::<JsonValue>::new());
    }

    #[test]
    fn exact_name() {
        assert_eq!(
            convert(&block_hash(), json!({"blockNumber": 1})).unwrap(),
            vec![json!(1)]
        );
        assert_eq!(
            convert(&state_call(), json!({"data": "0x", "method": "Core_version"})).unwrap(),
            vec![json!("Core_version"), json!("0x")]
        );
    }

    #[test]
    fn snake_form() {
        assert_eq!(
            convert(&block_hash(), json!({"block_number": 1})).unwrap(),
            vec![json!(1)]
        );
    }

    #[test]
    fn camel_form() {
        let params = vec![
            param("follow_subscription", false, &[]),
            param("operation_id", false, &[]),
        ];
        assert_eq!(
            convert(&params, json!({"followSubscription": "s", "operationId": "o"})).unwrap(),
            vec![json!("s"), json!("o")]
        );
    }

    #[test]
    fn alias_and_its_forms() {
        assert_eq!(convert(&block_hash(), json!({"hash": 1})).unwrap(), vec![json!(1)]);
        let params = vec![param("x", false, &["number_or_hash"])];
        assert_eq!(convert(&params, json!({"numberOrHash": 2})).unwrap(), vec![json!(2)]);
    }

    #[test]
    fn unknown_key_is_refused_naming_it_and_the_accepted_names() {
        let err = convert(&block_hash(), json!({"foo": 1})).unwrap_err();
        assert_eq!(err.code(), -32602);
        let data = err.data().unwrap().get().to_string();
        assert!(data.contains("\\\"foo\\\""), "{data}");
        assert!(data.contains("blockNumber (or hash)"), "{data}");

        let err = convert(&[], json!({"foo": 1})).unwrap_err();
        assert!(err.data().unwrap().get().contains("takes no params"));
    }

    #[test]
    fn echoed_key_is_truncated_to_64_chars() {
        let long = "k".repeat(1 << 20);
        let shown = format!("\\\"{}...\\\"", "k".repeat(64));

        let err = convert(&block_hash(), json!({ long.clone(): 1 })).unwrap_err();
        assert_eq!(err.code(), -32602);
        let data = err.data().unwrap().get();
        assert!(data.len() < 400, "{} bytes", data.len());
        assert!(data.contains(&shown), "{data}");

        // a method with no params, and the two-keys-for-one-param message
        let err = convert(&[], json!({ long.clone(): 1 })).unwrap_err();
        assert!(err.data().unwrap().get().len() < 400);
        let params = vec![param(&long, false, &["short"])];
        let err = convert(&params, json!({ long: 1, "short": 2 })).unwrap_err();
        let data = err.data().unwrap().get();
        assert!(data.contains("both name param"), "{}", &data[..200]);
        assert!(data.contains(&shown));

        // 64 characters is not 64 bytes, and a 64-character key is not cut
        assert_eq!(echo_key(&"é".repeat(65)), format!("{}...", "é".repeat(64)));
        assert_eq!(echo_key(&"k".repeat(64)), "k".repeat(64));
    }

    #[test]
    fn missing_required_param_is_refused() {
        let err = convert(&state_call(), json!({"data": "0x"})).unwrap_err();
        assert_eq!(err.code(), -32602);
        assert!(err
            .data()
            .unwrap()
            .get()
            .contains("missing required param \\\"method\\\""));
    }

    #[test]
    fn gap_before_last_supplied_param_is_null_when_optional() {
        let params = vec![
            param("key", false, &["prefix"]),
            param("count", false, &[]),
            param("startKey", true, &["start_key"]),
            param("at", true, &["hash"]),
        ];
        assert_eq!(
            convert(&params, json!({"prefix": "0x26", "count": 10, "hash": "0xab"})).unwrap(),
            vec![json!("0x26"), json!(10), JsonValue::Null, json!("0xab")]
        );
        // trailing omitted params are left out, not null
        assert_eq!(
            convert(&params, json!({"key": "0x26", "count": 10})).unwrap(),
            vec![json!("0x26"), json!(10)]
        );
    }

    #[test]
    fn two_keys_for_one_param_are_refused() {
        let err = convert(&state_call(), json!({"method": "A", "name": "B", "data": "0x"})).unwrap_err();
        assert_eq!(err.code(), -32602);
    }

    #[test]
    fn case_forms() {
        assert_eq!(snake_case("blockNumber"), "block_number");
        assert_eq!(snake_case("BlockHash"), "block_hash");
        assert_eq!(snake_case("number_or_hash"), "number_or_hash");
        assert_eq!(lower_camel_case("number_or_hash"), "numberOrHash");
        assert_eq!(lower_camel_case("BlockHash"), "blockHash");
        assert_eq!(lower_camel_case("childStorageKey"), "childStorageKey");
    }
}
