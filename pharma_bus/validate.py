"""Validate payloads and envelopes against the contracts.

Mirrors CDMOAgentContracts ``contracts/validate.py`` (same function names,
``Issue(path, message)`` and ``ContractValidationError``) and adds the pharma
envelope rule: for the 8 pharma contracts ``correlation_id`` must equal
``payload.asset_id``.

    from pharma_bus import validate, contract_errors, make_message, validate_message
    validate("StageGateDecision", payload)
    msg = make_message("CandidateNomination", payload, source_agent="drug-discovery-agent")
    validate_message(msg)
"""
from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from functools import lru_cache
from typing import Any

import jsonschema
from jsonschema import Draft202012Validator, FormatChecker

from .registry import LIBRARY_ONLY, UnknownContract, is_pharma, load_schema, registry, resolve_version
from .rules import semantic_errors

# ------------------------------------------------------------------ formats
FORMATS = FormatChecker(formats=())
_RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$")


@FORMATS.checks("date-time", raises=ValueError)
def _is_datetime(v: Any) -> bool:
    if not isinstance(v, str):
        return True
    if not _RFC3339.match(v):
        raise ValueError("expected RFC 3339 date-time with a timezone, e.g. 2026-11-05T14:30:00Z")
    datetime.fromisoformat(v.replace("z", "Z").replace("Z", "+00:00").replace("t", "T"))
    return True


@FORMATS.checks("date", raises=ValueError)
def _is_date(v: Any) -> bool:
    if isinstance(v, str):
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            raise ValueError("expected YYYY-MM-DD")
        date.fromisoformat(v)
    return True


@FORMATS.checks("uuid", raises=ValueError)
def _is_uuid(v: Any) -> bool:
    if isinstance(v, str):
        uuid.UUID(v)
        if len(v) != 36:
            raise ValueError("expected canonical 8-4-4-4-12 UUID")
    return True


@FORMATS.checks("uri-reference", raises=ValueError)
def _is_uri_ref(v: Any) -> bool:
    if isinstance(v, str) and re.search(r"\s", v):
        raise ValueError("URI must not contain whitespace")
    return True


# ------------------------------------------------------------------- errors
@dataclass(frozen=True)
class Issue:
    """One validation problem: a JSON path (``$.payload.asset_id``) and a message."""

    path: str
    message: str

    def __str__(self) -> str:
        return f"{self.path}: {self.message}"

    def as_dict(self) -> dict[str, str]:
        return {"path": self.path, "message": self.message}


class ContractValidationError(jsonschema.ValidationError):
    """Raised when data breaks a contract. ``.issues`` lists every problem;
    it is a ``jsonschema.ValidationError`` so existing handlers still work."""

    def __init__(self, contract: str, version: str, issues: list[Issue]):
        self.contract, self.version, self.issues = contract, version, issues
        lines = "\n  ".join(str(i) for i in issues)
        super().__init__(f"{contract} {version}: {len(issues)} problem(s)\n  {lines}")


def _path(parts) -> str:
    out = "$"
    for p in parts:
        out += f"[{p}]" if isinstance(p, int) else f".{p}"
    return out


@lru_cache(maxsize=None)
def validator_for(name: str, version: str) -> Draft202012Validator:
    return Draft202012Validator(load_schema(name, version), registry=registry(), format_checker=FORMATS)


def _leaf_errors(err) -> list:
    """Flatten anyOf/allOf context so messages point at the real field."""
    if err.context:
        out = []
        for e in err.context:
            out.extend(_leaf_errors(e))
        return out
    return [err]


def contract_errors(name: str, data: Any, version: str | None = None) -> list[Issue]:
    """Every problem with ``data`` as contract ``name`` ([] when valid)."""
    if name in LIBRARY_ONLY:
        raise UnknownContract(f"{name} holds shared definitions only; validate a message contract instead")
    v = resolve_version(name, version)
    issues: list[Issue] = []
    for err in sorted(validator_for(name, v).iter_errors(data), key=lambda e: list(e.absolute_path)):
        for leaf in _leaf_errors(err):
            issue = Issue(_path(leaf.absolute_path), leaf.message)
            if issue not in issues:
                issues.append(issue)
    if not issues:
        issues = [Issue(p, m) for p, m in semantic_errors(name, data)]
    return issues


def is_valid(name: str, data: Any, version: str | None = None) -> bool:
    return not contract_errors(name, data, version)


def validate(name: str, data: Any, version: str | None = None) -> bool:
    """Return True or raise ContractValidationError listing every problem."""
    issues = contract_errors(name, data, version)
    if issues:
        raise ContractValidationError(name, resolve_version(name, version), issues)
    return True


# ---------------------------------------------------------------- envelopes
def message_errors(message: Any) -> list[Issue]:
    """Envelope + payload problems; payload paths are prefixed ``$.payload``."""
    issues = contract_errors("MessageEnvelope", message)
    if issues:
        return issues
    mtype, ver = message["message_type"], message["schema_version"]
    try:
        exact = resolve_version(mtype, ver)
    except UnknownContract as exc:
        return [Issue("$.message_type" if "unknown contract" in str(exc) else "$.schema_version", str(exc))]
    if mtype in LIBRARY_ONLY or mtype == "MessageEnvelope":
        return [Issue("$.message_type", f"{mtype} cannot be sent as a payload")]
    out = [Issue("$.payload" + i.path[1:], i.message) for i in contract_errors(mtype, message["payload"], exact)]
    if is_pharma(mtype):
        asset_id = message["payload"].get("asset_id")
        corr = message.get("correlation_id")
        if corr is None:
            out.append(Issue("$.correlation_id", f"{mtype} messages need correlation_id = payload.asset_id"))
        elif asset_id is not None and corr != asset_id:
            out.append(Issue("$.correlation_id", f"correlation_id ({corr}) must equal payload.asset_id ({asset_id})"))
    return out


def validate_message(message: Any) -> bool:
    issues = message_errors(message)
    if issues:
        name = message.get("message_type", "?") if isinstance(message, dict) else "?"
        raise ContractValidationError(f"MessageEnvelope<{name}>", resolve_version("MessageEnvelope"), issues)
    return True


def utc_now() -> str:
    """Current UTC time as RFC 3339 with a Z suffix (seconds precision)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def make_message(message_type: str, payload: dict, source_agent: str, version: str | None = None,
                 **fields: Any) -> dict:
    """Wrap a payload in a valid envelope (new message_id, sent_at = now UTC).

    For pharma contracts ``correlation_id`` defaults to ``payload['asset_id']``."""
    if is_pharma(message_type) and "correlation_id" not in fields and isinstance(payload, dict) and "asset_id" in payload:
        fields["correlation_id"] = payload["asset_id"]
    msg = {
        "message_id": str(uuid.uuid4()),
        "message_type": message_type,
        "schema_version": resolve_version(message_type, version),
        "source_agent": source_agent,
        "sent_at": utc_now(),
        **fields,
        "payload": payload,
    }
    validate_message(msg)
    return msg
