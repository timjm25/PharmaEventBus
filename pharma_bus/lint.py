"""Repository self-check: ``pharma-bus check``.

Confirms every local schema is valid draft 2020-12, its ``$id`` and
``x-contract-version`` match its path, every ``$ref`` resolves, and each
message contract has a valid example in ``examples/``.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from .registry import LIBRARY_ONLY, PHARMA_CONTRACTS, catalog, load_schema, origin, registry, schema_id
from .validate import contract_errors


def _refs(node) -> Iterator[str]:
    if isinstance(node, dict):
        if isinstance(node.get("$ref"), str):
            yield node["$ref"]
        for v in node.values():
            yield from _refs(v)
    elif isinstance(node, list):
        for v in node:
            yield from _refs(v)


def examples_dir() -> Path:
    here = Path(__file__).resolve().parent
    for d in (here / "_examples", here.parent / "examples"):
        if d.is_dir():
            return d
    return here.parent / "examples"


def lint_schema(name: str, version: str) -> list[str]:
    """Problems with one schema file (empty list = clean)."""
    where = f"{name}/{version}.json"
    schema = load_schema(name, version)
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        return [f"{where}: not a valid 2020-12 schema: {exc.message}"]
    problems = []
    if schema.get("$id") != schema_id(name, version):
        problems.append(f"{where}: $id should be {schema_id(name, version)}")
    if schema.get("x-contract-version") != version:
        problems.append(f"{where}: x-contract-version should be {version}")
    resolver = registry().resolver(base_uri=schema_id(name, version))
    for ref in _refs(schema):
        try:
            resolver.lookup(ref)
        except Exception as exc:  # noqa: BLE001
            problems.append(f"{where}: $ref {ref} does not resolve ({exc.__class__.__name__})")
    return problems


def run(include_upstream: bool = False) -> list[str]:
    problems: list[str] = []
    for name, versions in catalog().items():
        for v in versions:
            if include_upstream or origin(name, v) == "local":
                problems.extend(lint_schema(name, v))
    ex = examples_dir()
    for name in catalog():
        if name in LIBRARY_ONLY or name == "MessageEnvelope" or origin(name) != "local":
            continue
        f = ex / f"{name}.json"
        if not f.exists():
            if name in PHARMA_CONTRACTS:
                problems.append(f"examples/{name}.json missing (every pharma contract needs a valid example)")
            continue
        for issue in contract_errors(name, json.loads(f.read_text(encoding="utf-8"))):
            problems.append(f"examples/{name}.json: {issue}")
    return problems
