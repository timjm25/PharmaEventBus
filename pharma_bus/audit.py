"""Audit logging to AuditTrailService (his P56), never crashing the caller.

``AuditClient.log(...)`` POSTs form fields to ``{AUDIT_TRAIL_URL}/log``. If the
service is unreachable or refuses the event, the event is appended to a local
JSONL file using the same SHA-256 hash chain as
``audittrailservice/audittrail/model.py`` (fields joined by ``\\x1f``, genesis
``"0" * 64``), so it can be replayed into the service later.

Flows take any callable with the keyword signature of :meth:`AuditClient.log`
(``AuditCallback``), so tests inject a list collector.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

GENESIS_HASH = "0" * 64
ACTOR_TYPES = ("human", "ai_system", "automated_process")
ACTIONS = ("AI_OUTPUT_GENERATED", "HUMAN_REVIEWED", "HUMAN_APPROVED", "HUMAN_OVERRIDDEN", "AUTO_LOG")


class AuditCallback(Protocol):
    def __call__(self, *, source_system: str, actor: str, actor_type: str, action: str, resource: str,
                 detail: str = "", related_event_id: int | None = None) -> Any: ...


def compute_hash(prev_hash: str, ts: str, source_system: str, actor: str, actor_type: str, action: str,
                 resource: str, detail: str, related_event_id: Any) -> str:
    """Identical to AuditTrailService ``_compute_hash``."""
    payload = "\x1f".join([prev_hash, ts, source_system, actor, actor_type, action, resource, detail,
                           "" if related_event_id is None else str(related_event_id)])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class AuditClient:
    """HTTP client for AuditTrailService with a local hash-chained JSONL fallback."""

    def __init__(self, base_url: str | None = None, fallback_path: str | os.PathLike = "data/audit_fallback.jsonl",
                 timeout: float = 3.0):
        self.base_url = (base_url or "").rstrip("/") or None
        self.fallback_path = Path(fallback_path)
        self.timeout = timeout
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls) -> "AuditClient":
        """``AUDIT_TRAIL_URL`` (unset = local file only) and ``PHARMA_BUS_AUDIT_FALLBACK``."""
        return cls(os.environ.get("AUDIT_TRAIL_URL"),
                   os.environ.get("PHARMA_BUS_AUDIT_FALLBACK", "data/audit_fallback.jsonl"))

    def __call__(self, **fields: Any) -> dict:
        return self.log(**fields)

    def log(self, *, source_system: str, actor: str, actor_type: str, action: str, resource: str,
            detail: str = "", related_event_id: int | None = None) -> dict:
        """Record one event. Returns ``{"mode": "remote"|"local", ...}``; never raises for transport errors."""
        if actor_type not in ACTOR_TYPES:
            raise ValueError(f"actor_type must be one of {ACTOR_TYPES}")
        if action not in ACTIONS:
            raise ValueError(f"action must be one of {ACTIONS}")
        fields = {"source_system": source_system, "actor": actor, "actor_type": actor_type, "action": action,
                  "resource": resource, "detail": detail,
                  "related_event_id": "" if related_event_id is None else str(related_event_id)}
        if self.base_url:
            try:
                data = urllib.parse.urlencode(fields).encode()
                req = urllib.request.Request(f"{self.base_url}/log", data=data, method="POST",
                                             headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:  # noqa: S310 - configured URL
                    if 200 <= resp.status < 300:
                        return {"mode": "remote", "status": resp.status}
            except (urllib.error.URLError, OSError, ValueError):
                pass
        return self._log_local(related_event_id=related_event_id, **{k: v for k, v in fields.items()
                                                                       if k != "related_event_id"})

    def _log_local(self, *, source_system: str, actor: str, actor_type: str, action: str, resource: str,
                   detail: str, related_event_id: int | None) -> dict:
        with self._lock:
            self.fallback_path.parent.mkdir(parents=True, exist_ok=True)
            prev = GENESIS_HASH
            if self.fallback_path.exists():
                lines = [ln for ln in self.fallback_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
                if lines:
                    prev = json.loads(lines[-1])["hash"]
            ts = datetime.now(timezone.utc).isoformat()
            h = compute_hash(prev, ts, source_system, actor, actor_type, action, resource, detail, related_event_id)
            event = {"ts": ts, "source_system": source_system, "actor": actor, "actor_type": actor_type,
                     "action": action, "resource": resource, "detail": detail,
                     "related_event_id": related_event_id, "prev_hash": prev, "hash": h}
            with self.fallback_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(event) + "\n")
        return {"mode": "local", **event}


def verify_local_chain(path: str | os.PathLike) -> bool:
    """True if every line of a fallback JSONL chains from the previous one."""
    prev = GENESIS_HASH
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        e = json.loads(line)
        if e["prev_hash"] != prev or compute_hash(prev, e["ts"], e["source_system"], e["actor"], e["actor_type"],
                                                  e["action"], e["resource"], e["detail"],
                                                  e["related_event_id"]) != e["hash"]:
            return False
        prev = e["hash"]
    return True
