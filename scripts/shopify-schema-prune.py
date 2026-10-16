#!/usr/bin/env python3
"""Builds the pruned Shopify Admin GraphQL schema the connector tests validate against.

The Shopify provider's query and mutation documents live in
`crates/service/src/connectors/providers/shopify/graphql/*.graphql`. Each one is
checked here against Shopify's published schema for the pinned API version, and
the part of that schema the documents touch is written to
`crates/service/tests/fixtures/shopify/schema-<version>.pruned.graphql`, which
the Rust test `shopify_graphql_test` validates every document against with no
network. A field, argument, type or required argument a document gets wrong
fails here (against the full schema) or in that test (against the pruned copy).

Source of the schema: Shopify's public introspection endpoint for the version,
https://shopify.dev/admin-graphql-direct-proxy/<version> (a POST of the standard
introspection query; no credentials). Pass a saved introspection JSON as the
first argument to work offline.

Every field a document selects is kept with ALL of its arguments (so a missing
required argument is still an error), and every input object and enum an
argument or variable names is kept whole. Object types keep only the fields the
documents select.

    python3 scripts/shopify-schema-prune.py [introspection.json]
"""
import glob
import json
import os
import re
import sys
import urllib.request

VERSION = "2026-10"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(ROOT, "crates/service/src/connectors/providers/shopify/graphql")
OUT = os.path.join(ROOT, f"crates/service/tests/fixtures/shopify/schema-{VERSION}.pruned.graphql")
URL = f"https://shopify.dev/admin-graphql-direct-proxy/{VERSION}"
BUILTIN = {"ID", "String", "Int", "Float", "Boolean"}

INTROSPECTION = """
query IntrospectionQuery {
  __schema {
    queryType { name } mutationType { name }
    types { ...FullType }
    directives { name locations args { ...InputValue } }
  }
}
fragment FullType on __Type {
  kind name
  fields(includeDeprecated: true) { name args { ...InputValue } type { ...TypeRef } }
  inputFields { ...InputValue }
  interfaces { ...TypeRef }
  enumValues(includeDeprecated: true) { name }
  possibleTypes { ...TypeRef }
}
fragment InputValue on __InputValue { name type { ...TypeRef } defaultValue }
fragment TypeRef on __Type { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } } } }
"""


def load_schema():
    if len(sys.argv) > 1:
        data = json.load(open(sys.argv[1]))
    else:
        request = urllib.request.Request(
            URL,
            data=json.dumps({"query": INTROSPECTION}).encode(),
            headers={"Content-Type": "application/json"},
        )
        data = json.load(urllib.request.urlopen(request, timeout=120))
    return data["data"]["__schema"]


TOKEN = re.compile(r'\s*(?:#[^\n]*|(?P<punct>[{}()\[\]:!$@=,])|(?P<name>[A-Za-z_][A-Za-z0-9_]*)|(?P<num>-?\d+(?:\.\d+)?)|(?P<str>"(?:[^"\\]|\\.)*"))')


def tokenize(text):
    out, pos = [], 0
    while pos < len(text):
        m = TOKEN.match(text, pos)
        if not m or m.end() == pos:
            if text[pos:].strip() == "":
                break
            raise SystemExit(f"cannot tokenize near {text[pos:pos+30]!r}")
        pos = m.end()
        for kind in ("punct", "name", "num", "str"):
            if m.group(kind) is not None and m.group(kind) != ",":
                out.append((kind, m.group(kind)))
    return out


class Parser:
    def __init__(self, tokens):
        self.t, self.i = tokens, 0

    def peek(self):
        return self.t[self.i] if self.i < len(self.t) else (None, None)

    def take(self, value=None):
        tok = self.t[self.i]
        if value is not None and tok[1] != value:
            raise SystemExit(f"expected {value!r}, found {tok[1]!r}")
        self.i += 1
        return tok

    def type_ref(self):
        if self.peek()[1] == "[":
            self.take("[")
            inner = self.type_ref()
            self.take("]")
            base = inner
        else:
            base = self.take()[1]
        if self.peek()[1] == "!":
            self.take("!")
        return base

    def value(self):
        kind, val = self.peek()
        if val == "$":
            self.take()
            self.take()
        elif val in ("[", "{"):
            depth = 0
            while True:
                v = self.take()[1]
                depth += v in "[{"
                depth -= v in "]}"
                if depth == 0:
                    break
        else:
            self.take()

    def arguments(self):
        names = []
        if self.peek()[1] == "(":
            self.take("(")
            while self.peek()[1] != ")":
                names.append(self.take()[1])
                self.take(":")
                self.value()
            self.take(")")
        return names

    def directives(self):
        found = []
        while self.peek()[1] == "@":
            self.take("@")
            found.append(self.take()[1])
            self.arguments()
        return found

    def selection_set(self):
        self.take("{")
        fields = []
        while self.peek()[1] != "}":
            name = self.take()[1]
            if self.peek()[1] == ":":
                self.take(":")
                name = self.take()[1]
            args = self.arguments()
            directives = self.directives()
            sub = self.selection_set() if self.peek()[1] == "{" else None
            fields.append((name, args, directives, sub))
        self.take("}")
        return fields

    def operation(self):
        kind = self.take()[1]
        self.take()  # name
        variables = []
        if self.peek()[1] == "(":
            self.take("(")
            while self.peek()[1] != ")":
                self.take("$")
                self.take()
                self.take(":")
                variables.append(self.type_ref())
                if self.peek()[1] == "=":
                    self.take("=")
                    self.value()
            self.take(")")
        self.directives()
        return kind, variables, self.selection_set()


def named(ref):
    while ref.get("name") is None:
        ref = ref["ofType"]
    return ref["name"]


def render_type(ref):
    if ref["kind"] == "NON_NULL":
        return render_type(ref["ofType"]) + "!"
    if ref["kind"] == "LIST":
        return "[" + render_type(ref["ofType"]) + "]"
    return ref["name"]


def main():
    schema = load_schema()
    types = {t["name"]: t for t in schema["types"]}
    keep_fields = {}  # object type -> set(field names)
    whole = set()  # input objects, enums, scalars kept in full
    directives_used = set()

    def keep_whole(name):
        if name in BUILTIN or name in whole:
            return
        t = types[name]
        whole.add(name)
        if t["kind"] == "INPUT_OBJECT":
            for f in t["inputFields"]:
                keep_whole(named(f["type"]))

    def walk(type_name, fields):
        t = types[type_name]
        by_name = {f["name"]: f for f in t["fields"]}
        for name, _args, directives, sub in fields:
            if name == "__typename":
                continue
            if name not in by_name:
                raise SystemExit(f"{type_name} has no field {name}")
            definition = by_name[name]
            keep_fields.setdefault(type_name, set()).add(name)
            directives_used.update(directives)
            for arg in definition["args"]:
                keep_whole(named(arg["type"]))
            target = named(definition["type"])
            kind = types[target]["kind"]
            if kind in ("OBJECT", "INTERFACE"):
                if sub is None:
                    raise SystemExit(f"{type_name}.{name} needs a selection")
                walk(target, sub)
            else:
                if sub is not None:
                    raise SystemExit(f"{type_name}.{name} is a leaf")
                keep_whole(target)

    roots = {"query": schema["queryType"]["name"], "mutation": schema["mutationType"]["name"]}
    files = sorted(glob.glob(os.path.join(DOCS, "*.graphql")))
    if not files:
        raise SystemExit("no documents")
    used_roots = set()
    for path in files:
        text = open(path).read().replace("{{KEY}}", "00000000-0000-4000-8000-000000000000")
        kind, variables, selection = Parser(tokenize(text)).operation()
        used_roots.add(kind)
        for variable_type in variables:
            keep_whole(variable_type)
        walk(roots[kind], selection)

    lines = [
        f"# Pruned from Shopify's published Admin GraphQL schema, API version {VERSION}.",
        f"# Source: {URL} (introspection). Generated by scripts/shopify-schema-prune.py.",
        "# Contains only what the connector's documents use; do not edit by hand.",
        "",
        "schema {",
    ]
    for kind in ("query", "mutation"):
        if kind in used_roots:
            lines.append(f"  {kind}: {roots[kind]}")
    lines.append("}")
    lines.append("")
    for d in schema["directives"]:
        if d["name"] in directives_used:
            args = ", ".join(f"{a['name']}: {render_type(a['type'])}" for a in d["args"])
            lines.append(f"directive @{d['name']}({args}) on {' | '.join(d['locations'])}")
    lines.append("")
    emitted = set(keep_fields) | whole
    for name in sorted(emitted):
        t = types[name]
        if name in whole:
            if t["kind"] == "SCALAR":
                lines.append(f"scalar {name}")
            elif t["kind"] == "ENUM":
                lines.append(f"enum {name} {{ " + " ".join(v["name"] for v in t["enumValues"]) + " }")
            elif t["kind"] == "INPUT_OBJECT":
                lines.append(f"input {name} {{")
                for f in t["inputFields"]:
                    lines.append(f"  {f['name']}: {render_type(f['type'])}")
                lines.append("}")
        else:
            lines.append(f"type {name} {{")
            for f in t["fields"]:
                if f["name"] in keep_fields[name]:
                    args = ""
                    if f["args"]:
                        args = "(" + ", ".join(f"{a['name']}: {render_type(a['type'])}" for a in f["args"]) + ")"
                    lines.append(f"  {f['name']}{args}: {render_type(f['type'])}")
            lines.append("}")
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    open(OUT, "w").write("\n".join(lines) + "\n")
    print(f"{len(files)} documents, {len(emitted)} types -> {os.path.relpath(OUT, ROOT)}")


if __name__ == "__main__":
    main()
