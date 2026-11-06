"""Discover, load and resolve the versioned contract schemas.

Same layout and URN namespace as CDMOAgentContracts, so the two schema sets
can merge into one registry: ``schemas/<Contract>/<MAJOR.MINOR.PATCH>.json``
with ``$id`` = ``urn:cdmo-agent-contracts:<Contract>:<version>``.

Sources, in order:

1. This repo's ``schemas/`` (the 8 pharma contracts, ``PharmaCommon`` and the
   vendored upstream ``Common``, ``MessageEnvelope`` and ``CapacityRequest``).
2. The upstream CDMOAgentContracts schema folder, when available: the
   installed ``contracts`` package (pip name ``cdmo-agent-contracts``) or, if
   that is not installed, the folder named by ``CDMO_CONTRACTS_SCHEMA_DIR``
   (a checkout root or its ``schemas/`` folder). This lets manufacturing
   messages (BatchRecord, DeviationReport, ReleaseRequest ...) validate on the
   same bus. Set ``PHARMA_BUS_DISABLE_UPSTREAM=1`` to switch this off.

A contract version present in both places is taken from this repo (the
vendored copy is byte-identical to upstream; ``tests/test_upstream_sync.py``
checks that).
"""
from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path
from types import ModuleType

from referencing import Registry
from referencing.jsonschema import DRAFT202012

URN_PREFIX = "urn:cdmo-agent-contracts:"
SEMVER = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
#: Contracts that hold only shared definitions, never a message on their own.
LIBRARY_ONLY = {"Common", "PharmaCommon"}
#: The sponsor-side contracts this repo owns. Each payload carries asset_id.
PHARMA_CONTRACTS = (
    "CandidateNomination", "AssayResult", "StudyReport", "StageGateDecision",
    "SubmissionPackage", "ValuationSnapshot", "PartnerDeliverable", "DealTerms",
)
#: Files copied byte-for-byte from timjm25/CDMOAgentContracts (see vendor/UPSTREAM.md).
VENDORED = ("Common/1.0.0.json", "MessageEnvelope/1.0.0.json", "CapacityRequest/1.0.0.json")
UPSTREAM_REPO = "timjm25/CDMOAgentContracts"
UPSTREAM_COMMIT = "8c7bf2c"


class UnknownContract(KeyError):
    """Raised for an unknown contract name or a version that does not exist."""

    def __str__(self) -> str:  # KeyError quotes its arg; keep the message readable
        return self.args[0]


def parse_version(v: str) -> tuple[int, int, int]:
    m = SEMVER.match(v)
    if not m:
        raise ValueError(f"{v!r} is not a MAJOR.MINOR.PATCH version")
    return tuple(int(x) for x in m.groups())  # type: ignore[return-value]


def schema_dir() -> Path:
    """This repo's schema folder (installed wheel first, then source checkout)."""
    here = Path(__file__).resolve().parent
    for d in (here / "_schemas", here.parent / "schemas"):
        if d.is_dir():
            return d
    raise FileNotFoundError("pharma_bus schemas not found (expected pharma_bus/_schemas or ./schemas)")


def _normalise_upstream(path: Path) -> Path | None:
    if (path / "schemas").is_dir():
        path = path / "schemas"
    return path if (path / "Common").is_dir() else None


def upstream_package() -> ModuleType | None:
    """The installed CDMOAgentContracts ``contracts`` package, or None.

    Checks for the attributes we use, so an unrelated package that happens to
    be called ``contracts`` is ignored."""
    try:
        import contracts  # type: ignore[import-not-found]
        from contracts import registry as up_registry  # type: ignore[import-not-found]
    except Exception:  # noqa: BLE001 - any import problem means "not available"
        return None
    if not hasattr(up_registry, "schema_dir") or not hasattr(contracts, "message_errors"):
        return None
    return contracts


def upstream_schema_dir() -> Path | None:
    """Folder of upstream CDMOAgentContracts schemas to merge in, or None."""
    if os.environ.get("PHARMA_BUS_DISABLE_UPSTREAM", "").strip() in ("1", "true", "yes"):
        return None
    pkg = upstream_package()
    if pkg is not None:
        try:
            d = _normalise_upstream(Path(pkg.registry.schema_dir()))
            if d:
                return d
        except Exception:  # noqa: BLE001
            pass
    env = os.environ.get("CDMO_CONTRACTS_SCHEMA_DIR")
    if env and Path(env).is_dir():
        return _normalise_upstream(Path(env))
    return None


def _scan(base: Path) -> dict[str, dict[str, Path]]:
    out: dict[str, dict[str, Path]] = {}
    for d in sorted(p for p in base.iterdir() if p.is_dir() and not p.name.startswith(("_", "."))):
        for f in d.glob("*.json"):
            if SEMVER.match(f.stem):
                out.setdefault(d.name, {})[f.stem] = f
    return out


@lru_cache(maxsize=None)
def _files() -> dict[str, dict[str, tuple[Path, str]]]:
    """{name: {version: (path, origin)}} where origin is 'local' or 'upstream'."""
    out: dict[str, dict[str, tuple[Path, str]]] = {}
    for name, versions in _scan(schema_dir()).items():
        out[name] = {v: (f, "local") for v, f in versions.items()}
    up = upstream_schema_dir()
    if up is not None:
        for name, versions in _scan(up).items():
            for v, f in versions.items():
                out.setdefault(name, {}).setdefault(v, (f, "upstream"))
    return out


def catalog() -> dict[str, list[str]]:
    """{contract name: [versions, oldest first]} across local and upstream schemas."""
    return {name: sorted(vs, key=parse_version) for name, vs in sorted(_files().items())}


def origin(name: str, version: str | None = None) -> str:
    """'local' or 'upstream' for a contract version."""
    v = resolve_version(name, version)
    return _files()[name][v][1]


def list_contracts(include_library: bool = False) -> dict[str, list[str]]:
    return {k: v for k, v in catalog().items() if include_library or k not in LIBRARY_ONLY}


def is_pharma(name: str) -> bool:
    return name in PHARMA_CONTRACTS


def resolve_version(name: str, version: str | None = None) -> str:
    """None -> latest; '1' -> latest 1.x.x; '1.1' -> latest 1.1.x; '1.1.0' exact."""
    cat = catalog()
    if name not in cat:
        raise UnknownContract(f"unknown contract {name!r}; known: {', '.join(list_contracts())}")
    versions = cat[name]
    if version is None:
        return versions[-1]
    parts = str(version).split(".")
    matches = [v for v in versions if v.split(".")[: len(parts)] == parts]
    if not matches:
        raise UnknownContract(f"{name} has no version matching {version!r}; available: {', '.join(versions)}")
    return matches[-1]


@lru_cache(maxsize=None)
def _load(name: str, version: str) -> dict:
    return json.loads(_files()[name][version][0].read_text(encoding="utf-8"))


def load_schema(name: str, version: str | None = None) -> dict:
    return _load(name, resolve_version(name, version))


def schema_path(name: str, version: str | None = None) -> Path:
    v = resolve_version(name, version)
    return _files()[name][v][0]


def schema_id(name: str, version: str) -> str:
    return f"{URN_PREFIX}{name}:{version}"


@lru_cache(maxsize=None)
def registry() -> Registry:
    resources = []
    for name, versions in catalog().items():
        for v in versions:
            schema = _load(name, v)
            resources.append((schema.get("$id", schema_id(name, v)), DRAFT202012.create_resource(schema)))
    return Registry().with_resources(resources)


def clear_cache() -> None:
    """Forget loaded schemas (call after changing env vars or schema files)."""
    for f in (_files, _load, registry):
        f.cache_clear()
    try:
        from .validate import validator_for
        validator_for.cache_clear()
    except ImportError:  # pragma: no cover - during partial import
        pass
