#!/usr/bin/env python3
"""Extract the node's JSON-RPC argument names from jsonrpsee `#[rpc]` traits.

A Substrate/Frontier node built on jsonrpsee accepts by-name (object) params
keyed by the Rust argument name (or its `#[argument(rename = "..")]`), plus the
snake_case and lowerCamelCase forms of it (jsonrpsee-proc-macros 0.24,
render_server.rs `decode_map`). This script reads the trait sources and prints
the fixture that tests/node_param_names.json pins:

    {"methods": {"<rpc method>": ["<arg>", ...], ...}}

An argument of type Option<..> is written with a trailing "?": jsonrpsee
lets a caller leave it out.

Usage (source roots at the revisions the deployed node is built from):

    python3 scripts/extract_node_param_names.py \
        --polkadot-sdk <polkadot-sdk source root> \
        --subtensor <subtensor source root> > tests/node_param_names.json

then put the revisions back in the "sources" block (the script does not know
them). Only the files listed in SOURCES are read.
"""

import argparse
import json
import re
import sys
from pathlib import Path

SOURCES = {
    "polkadot-sdk": [
        "substrate/client/rpc-api/src/author/mod.rs",
        "substrate/client/rpc-api/src/chain/mod.rs",
        "substrate/client/rpc-api/src/child_state/mod.rs",
        "substrate/client/rpc-api/src/state/mod.rs",
        "substrate/client/rpc-api/src/system/mod.rs",
        "substrate/client/rpc-api/src/offchain/mod.rs",
        "substrate/utils/frame/rpc/system/src/lib.rs",
        "substrate/client/rpc-spec-v2/src/chain_spec/api.rs",
        "substrate/client/rpc-spec-v2/src/archive/api.rs",
        "substrate/client/rpc-spec-v2/src/chain_head/api.rs",
        "substrate/client/rpc-spec-v2/src/transaction/api.rs",
    ],
    "subtensor": [
        "pallets/subtensor/rpc/src/lib.rs",
        "pallets/swap/rpc/src/lib.rs",
        "vendor/frontier/client/rpc-core/src/eth.rs",
        "vendor/frontier/client/rpc-core/src/eth_pubsub.rs",
        "vendor/frontier/client/rpc-core/src/net.rs",
        "vendor/frontier/client/rpc-core/src/web3.rs",
        "vendor/frontier/client/rpc-core/src/txpool.rs",
        "vendor/frontier/client/rpc-core/src/debug.rs",
    ],
}


def strip_comments(src: str) -> str:
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"//[^\n]*", "", src)


def balanced(src: str, start: int) -> int:
    """Index just past the bracket group that opens at src[start]."""
    close = {"(": ")", "[": "]", "{": "}", "<": ">"}
    stack = []
    i = start
    while i < len(src):
        c = src[i]
        if c == '"':
            i = src.index('"', i + 1)
        elif c in close:
            stack.append(close[c])
        elif stack and c == stack[-1]:
            stack.pop()
            if not stack:
                return i + 1
        i += 1
    raise ValueError("unbalanced")


def split_top(s: str) -> list[str]:
    out, depth, cur = [], 0, ""
    for c in s:
        if c in "([{<":
            depth += 1
        elif c in ")]}>":
            depth -= 1
        if c == "," and depth == 0:
            out.append(cur)
            cur = ""
        else:
            cur += c
    if cur.strip():
        out.append(cur)
    return out


ATTR = re.compile(r"#\[(method|subscription)\(")
FN = re.compile(r"\bfn\s+\w+\s*(<[^>]*>)?\s*\(")


def extract(src: str) -> dict[str, list[str]]:
    src = strip_comments(src)
    found = {}
    for m in ATTR.finditer(src):
        attr_end = balanced(src, m.end() - 1)
        attr = src[m.end() : attr_end - 1]
        name = re.search(r'\bname\s*=\s*"([^"]+)"', attr).group(1)
        alias_list = re.search(r"\baliases\s*=\s*\[([^\]]*)\]", attr)
        aliases = re.findall(r'"([^"]+)"', alias_list.group(1)) if alias_list else []
        fn = FN.search(src, attr_end)
        args_end = balanced(src, fn.end() - 1)
        names = []
        for arg in split_top(src[fn.end() : args_end - 1]):
            arg = arg.strip()
            rename = re.search(r'#\[argument\(\s*rename\s*=\s*"([^"]+)"', arg)
            arg = re.sub(r"#\[[^\]]*\]", "", arg).strip()
            if not arg or re.fullmatch(r"&\s*(mut\s+)?self", arg):
                continue
            pat, ty = (x.strip() for x in arg.split(":", 1))
            pat = re.sub(r"^mut\s+", "", pat)
            optional = "?" if re.match(r"(\w+::)*Option\s*<", ty) else ""
            names.append((rename.group(1) if rename else pat) + optional)
        for n in [name, *aliases]:
            found[n] = names
    return found


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--polkadot-sdk", required=True, type=Path)
    ap.add_argument("--subtensor", required=True, type=Path)
    a = ap.parse_args()
    roots = {"polkadot-sdk": a.polkadot_sdk, "subtensor": a.subtensor}
    methods = {}
    for repo, files in SOURCES.items():
        for f in files:
            methods.update(extract((roots[repo] / f).read_text()))
    json.dump({"methods": dict(sorted(methods.items()))}, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
